import java.io.*;
import java.net.*;
import java.lang.reflect.*;
import java.util.*;

import java.lang.annotation.*;

public class MyClient {
	public static void main(String args[]) {
		new MyClient();
	}

	public static final String ANSI_RESET 	= "\u001B[0m";
	public static final String ANSI_BLACK 	= "\u001B[30m";
	public static final String ANSI_RED 	= "\u001B[31m";
	public static final String ANSI_GREEN 	= "\u001B[32m";
	public static final String ANSI_YELLOW 	= "\u001B[33m";
	public static final String ANSI_BLUE 	= "\u001B[34m";
	public static final String ANSI_PURPLE 	= "\u001B[35m";
	public static final String ANSI_CYAN 	= "\u001B[36m";
	public static final String ANSI_WHITE 	= "\u001B[37m";

	private EnumSysLogLevel sysMessageLogLevel = EnumSysLogLevel.Sys;

	private Map<String, MethodCallData> commandMap = new HashMap<String, MethodCallData>();

	private boolean runClient = false;

	// IO Objects
	private Socket socket;
	private BufferedReader inputStream;
	private DataOutputStream outputStream;

	private Class<?> dataRecordType = null;

	public Object[] response_data = null;

	private boolean jobCompleted = false;

	// List of servers, keeps track of servers and jobs queued on them.
	private Map<String, ServerList> servers;

	private Scheduler scheduler;

	private boolean quit = false;

	public JobQueue incomingJobs = new JobQueue();

	private Job recentlyCompletedJob = null;
	private ComputeServer serverWithRecentlyCompletedJob = null;

	public MyClient() {
		mapCommands();

		try {
			initSocket();
			runClient = true;
		} catch (Exception ex) {
			runClient = false;
		}

		if (runClient) {
			// scheduler = new RoundRobinScheduler();
			scheduler = new Part2Scheduler();

			C_AuthHandshake();

			// Initial ready message
			while (runClient) {
				run();
			}
		}

		closeSocket();
	}

	private void run() {
		C_Ready();

		handleNextMessage(); // Typically JOBN, JCPL, NONE

		if (!scheduler.initialised())
			scheduler.initialise(this);

		if (quit)
			handleNextMessage();
		else {
			// Run the scheduler with most recent job.
			scheduler.run();
		}
	}

	public void loadServerInfo() {
		C_GetServerState(EnumGETSState.All, null, null);

		// handleNextMessage(); // DATA

		// Assume there are at least 3 servers per type,
		// That way we don't allocate too much or too little space causing reallocations
		// and copies.
		servers = new HashMap<String, ServerList>(response_data.length / 3);

		ServerState[] svrs = getDataResponse();

		int order = 0;
		for (ServerState serverState : svrs) {
			// If we already have this type registered, add this server to the array.
			// otherwise add new type mapping and server to dictionary.
			var s = servers.get(serverState.type);
			if (s != null) {
				s.servers.add(new ComputeServer(serverState, s));
			} else {
				servers.put(serverState.type, new ServerList(order, new ComputeServer(serverState, s)));
				order++;
			}
		}

		handleNextMessage(); // .
	}

	/**
	 * Used to cache the expected object type that will be read in by the next DATA
	 * message
	 * 
	 * @param type
	 */
	public void setDataRecordType(Class<?> type) {
		this.dataRecordType = type;
	}

	/***************************************************************************************************
	 * 
	 * Command Discover & Execution
	 * 
	 **************************************************************************************************/

	/**
	 * Creates a map of all methods annotated with the Command annotation type.
	 */
	private void mapCommands() {
		final Class<ServerCommand> commandAnnotationClass = ServerCommand.class;
		int count = 0;

		for (Method m : this.getClass().getDeclaredMethods()) {
			var cmdat = m.getAnnotation(commandAnnotationClass);

			if (cmdat != null) {
				commandMap.put(cmdat.cmd(), new MethodCallData(m, m.getParameters()));

				writeConsoleSysMsg(EnumSysLogLevel.Full, "Mapped command ", cmdat.cmd());

				count++;
			}
		}

		writeConsoleSysMsg(EnumSysLogLevel.Info,
				new StringBuilder().append("Mapped ").append(count).append(" commands."));
	}

	private void findAndExecuteCommand(String args) {
		final int CMD_IDX = 0;
		var splitArgs = args.split(" ");

		// Try get the command, if it doesn't exist print error and return.
		var commandMethod = commandMap.get(splitArgs[CMD_IDX]);
		if (commandMethod == null) {
			writeConsoleErrMsg(EnumSysLogLevel.Full, "Command ", splitArgs[CMD_IDX], " is invalid.");
			return;
		}

		// Prepare arguments for execution
		String[] argsToPass = new String[splitArgs.length - 1];

		// Copy args minus cmd
		for (int i = 1; i <= splitArgs.length - 1; i++)
			argsToPass[i - 1] = splitArgs[i];

		executeCommand(commandMethod, argsToPass);
	}

	public void handleNextMessage() {
		findAndExecuteCommand(read());
	}

	/*
	 * Get params
	 * very args and params length matches
	 * cast all args to param types
	 * invoke method with typecast args
	 */
	private void executeCommand(MethodCallData m, String[] args) {
		// If we didnt receive enough params, print error and return.
		if (args.length != m.parameters.length) {
			writeConsoleSysMsg(EnumSysLogLevel.Full,
					"Cannot invoke ",
					m.method.getName(),
					", expected ",
					m.parameters.length,
					" parameters, got ",
					args.length);
			return;
		}

		List<Object> castParams = new ArrayList<Object>(m.parameters.length);
		int argIdx = 0;
		var sb = new StringBuilder(96);

		for (Parameter p : m.parameters) {
			var pType = p.getType();

			// Cast argument to function parameter
			var castType = castArgument(pType, args[argIdx++]);
			if (castType == null) {
				writeConsoleErrMsg(EnumSysLogLevel.Full, "Cannot cast param ", p.getName(), " of type ", pType);
			} else {
				castParams.add(castType);
			}

			sb.append("\t").append(p.getName()).append(": ").append(castType).append("\n");
		}

		try {
			writeConsoleSysMsg(EnumSysLogLevel.Full, new StringBuilder("Invoking: ").append(m.method.getName()));

			// If we have some arguments, then print them out if running with
			// SysLogLevel.Full verbosity.
			if (sb.length() > 0)
				writeConsoleSysMsg(EnumSysLogLevel.Full, sb);

			// Invoke the method with params
			m.method.invoke(this, castParams.toArray());
		} catch (Exception ex) {
			writeConsoleSysMsg(EnumSysLogLevel.Full,
					"Failed to invoke ",
					m.method.getName(),
					"\n",
					ex.getMessage());
		}
	}

	private Object castArgument(Class<?> type, String arg) {
		// String
		if (type.equals(String.class)) {
			return arg;
		}
		// Integers
		else if (type.equals(int.class)) {
			return Integer.parseInt(arg);
		}
		// Floats
		else if (type.equals(float.class)) {
			return Float.parseFloat(arg);
		}
		return null;
	}

	/***************************************************************************************************
	 * 
	 * Client Messages
	 * 
	 **************************************************************************************************/

	public void C_AuthHandshake() {
		// Auth handshake
		write("HELO");
		handleNextMessage(); // OK
		write("AUTH", System.getProperty("user.name"));
		handleNextMessage(); // OK
	}

	/*
	 * The REDY command signals ds-server for a next simulation event.
	 */
	@ClientCommand(cmd = "REDY")
	public void C_Ready() {
		write("REDY");
	}

	/**
	 * The GETS command queries server state information at the current simulation
	 * time. The All option requests the information on all servers regardless of
	 * their state including inactive and unavailable. The Type
	 * option requests the information on servers of a specified type (serverType)
	 * regardless of their state, too. The
	 * Capable and Avail options make requests for server state information based on
	 * initial resource capacity and
	 * the current resource availability, respectively. For instance, GETS Capable 3
	 * 500 1000 and GETS Avail 3 500
	 * 1000 are different in that the response to the former is all servers that can
	 * “eventually” provide 3 cores, 500MB
	 * of memory and 1000MB of disk regardless of their current availability.
	 * Meanwhile, the response to the latter
	 * is all servers that can “immediately” provide 3 cores, 500MB of memory and
	 * 1000MB of disk. With the Avail
	 * option, if there are insufficient available resources and/or waiting jobs,
	 * the server is not available for the job.
	 * In general, it is recommended to use the Capable option than the Avail option
	 * as the system is often busy,
	 * i.e., all servers are running one or more jobs at any given point in time. In
	 * the case of no servers are available
	 * (Avail), the message right after the DATA message will be ‘.’
	 */
	@ClientCommand(cmd = "GETS")
	public void C_GetServerState(EnumGETSState state, String type, Applicance sys) {
		var sb = new StringBuilder("GETS ").append(state.getLabel());
		switch (state) {
			case All:
				break;
			case Type:
				if (type == null) {
					writeConsoleSysMsg(EnumSysLogLevel.Full, "GETS requires param type to be specified with flag Type");
					return;
				}
				sb.append(type);
				break;
			case Capable:
			case Available:
				if (sys == null) {
					writeConsoleSysMsg(EnumSysLogLevel.Full, "GETS requires param sys to be specified with flag ",
							state.getLabel());
					return;
				}
				sb.append(sys.toString());
				break;
		}
		setDataRecordType(ServerState.class);
		write(sb);

		handleNextMessage(); // Data
	}

	/**
	 * The SCHD command schedules a job (jobID) to the server (serverID) of
	 * serverType.
	 * {@code SCHD 3 joon 1}
	 */
	@ClientCommand(cmd = "SCHD")
	public void C_Schedule(Job job, String serverType, int serverId) {
		var svr = servers.get(serverType).servers.get(serverId);

		// If the server currently has no jobs in its queue, we will default the job
		// state to running.
		if (svr.jobs.isEmpty())
			job.state = EnumJobState.Running;
		else // Otherwise the state will be submitted.
			job.state = EnumJobState.Submitted;

		svr.jobs.enqueue(job);

		write("SCHD", job.jobId, serverType, serverId);

		handleNextMessage(); // Receive OK
	}

	/**
	 * The ENQJ command places the current job to a specified queue. The name used
	 * for the global queue is GQ.
	 * {@code ENQJ GQ}
	 * 
	 * @param queueName Queue to add current job to.
	 */
	@ClientCommand(cmd = "ENQJ")
	public void C_EnqueueJob(String queueName) {
		write("ENQJ", queueName);
	}

	/**
	 * The DEQJ command gets the job at the specified queue position (qID) from the
	 * specified queue.
	 * {@code DEQJ GQ 2 // job at the third place (qID of 2 starting from 0) of the global queue}
	 * 
	 * @param queueName
	 * @param queuePosition
	 */
	@ClientCommand(cmd = "DEQJ")
	public void C_DequeueJob(String queueName, int queuePosition) {
		write("DEQJ ", queueName);
	}

	/**
	 * The LSTQ command gets job information in the specified queue.
	 * LSTQ queue name i|$|#|*
	 * 
	 * @param queueName Queue to get info for
	 * @param type      Index | Number | All | $ (Not used)
	 * @param i         Used when Index is specified.
	 */
	@ClientCommand(cmd = "LSTQ")
	public void C_GetJobInfo(String queueName, EnumListType type, Integer i) {
		var sb = new StringBuilder("LSTQ ").append(queueName).append(' ');

		if (type == EnumListType.Index) {
			if (i == null) {
				writeConsoleErrMsg(EnumSysLogLevel.Full, "Cannot get job info for queue: ", queueName,
						" with Index type when i is null");
				return;
			}

			sb.append(i.intValue());
		} else {
			sb.append(type.getCommand());
		}

		write(sb);

		// Handle responses because of course they have no header and have different
		// formats...
		switch (type) {
			case Index:
				Job response = read(Job.class);
				break;
			case Number:
				var numberOfJobsInQueue = Integer.valueOf(read());
				break;
			case All:
				setDataRecordType(Job.class);
				break;
			case Dollar: // Currently unused
				break;
		}

	}

	/**
	 * The CNTJ command queries the number of jobs of a specified state, on a
	 * specified server.
	 * The job state is specified by one of state codes, except 0 for ‘submitted’.
	 * {@code
	 * CNTJ joon 0 2 // query the number of running jobs on joon 0
	 * 1 // the response from ds-server, i.e., 1 running job on joon 0
	 * }
	 * 
	 * @param serverType
	 * @param serverId
	 * @param jobState
	 */
	@ClientCommand(cmd = "CNTJ")
	public void C_JobCountForServer(String serverType, int serverId, int jobState) { // TODO Jobstate enum
		write("CNTJ", serverType, serverId, jobState);

		// read a number
		var answer = readInt();
	}

	/**
	 * The EJWT command queries the sum of estimated waiting times on a given
	 * server. It does not take into
	 * account the remaining runtime of running jobs. Note that the calculation
	 * should not be considered to be
	 * accurate because (1) it is based on estimated runtimes of waiting jobs and
	 * (2) more importantly, it does not
	 * consider the possibility of parallel execution of waiting jobs.
	 * 
	 * @param serverType
	 * @param serverId
	 */
	@ClientCommand(cmd = "EJWT")
	public void C_EstimatedJobWaitingTime(String serverType, int serverId) {
		write("EJWT", serverType, serverId);

		// read a number
		var answer = readInt();
	}

	/**
	 * The LSTJ command queries the list of running and waiting jobs on a given
	 * server. The response to LSTJ
	 * is formatted in jobID jobState submitTime startTime estRunTime core memory
	 * disk. The job state is sent as
	 * a state code either 1 or 2 for waiting and running, respectively. The
	 * response will be a sequence of DATA, a
	 * series of job information and OK message pairs and ‘.’.
	 * 
	 * @param serverType
	 * @param serverId
	 */
	@ClientCommand(cmd = "LSTJ")
	public void C_ListJobs(String serverType, int serverId) {
		write("LSTJ", serverType, serverId);
		setDataRecordType(JobStatus.class);
		handleNextMessage(); // Data
	}

	/**
	 * The MIGJ command migrates a job specified by jobID on srcServerID of
	 * srcServerType to tgtServerID of
	 * tgtServerType. The job can be of waiting, running or suspended. The
	 * successful migration results in the same
	 * behaviour of normal scheduling action. In particular, the job’s state on the
	 * target server is determined by
	 * the common criteria of job execution, such as the resource availability and
	 * running/waiting jobs of the target
	 * server. The migrated job will “restart” on the target server.
	 * 
	 * @param jobId
	 * @param srcServerType
	 * @param srcServerId
	 * @param targetServerType
	 * @param targetServerId
	 */
	@ClientCommand(cmd = "MIGJ")
	public void C_MigrateJob(int jobId, String srcServerType, int srcServerId, String targetServerType,
			int targetServerId) {
		write("MIGJ", jobId, srcServerType, srcServerId, targetServerType, targetServerId);

		//find dest server and add job to queue
		var svrLst = this.servers.get(targetServerType);
		ComputeServer targetServer = null;
		for (var svr : svrLst.servers) {
			if (svr.server.id != targetServerId)
				continue;
			
			targetServer = svr;
			break;
		}

		// find source server and remove job from queue.
		svrLst = this.servers.get(srcServerType);
		for (int i = 0; i < svrLst.servers.size(); i++) {
			var s = svrLst.servers.get(i);
			if (s.server.id != srcServerId)
				continue;

			for (int j = 0; j < s.jobs.size(); j++) {
				var job = s.jobs.get(j);
				if (job.jobId == jobId) {
					targetServer.jobs.add(job); // Add to destination server
					s.jobs.remove(j); // Remove from source server
					break;
				}
			}
			break;
		}

		// RX OK
		handleNextMessage();
	}

	/**
	 * The KILJ command kills a job. The killed job is pushed back to the queue with
	 * the killed time as a new
	 * submission time. The job will be resubmitted with JOBP.
	 * 
	 * @param serverType
	 * @param serverId
	 * @param jobId
	 */
	@ClientCommand(cmd = "KILJ")
	public void C_KillJob(String serverType, int serverId, int jobId) {
		write("KILJ", serverType, serverId, jobId);

		// RX OK
		handleNextMessage();
	}

	/**
	 * The TERM command terminates a server. All waiting/running jobs are killed and
	 * re-submitted
	 * for scheduling with JOBP. The server is then put into the inactive state.
	 * 
	 * @param serverType
	 * @param serverId
	 */
	@ClientCommand(cmd = "TERM")
	public void C_TerminateServer(String serverType, int serverId) {
		write("TERM", serverType, serverId);

		// Read number of jobs killed
		var jobsKilled = readInt();
	}

	@ClientCommand(cmd = "QUIT")
	public void C_Quit() {
		write("QUIT");
		quit = true;
	}

	@ClientCommand(cmd = "OK")
	public void C_OK() {
		write("OK");
	}

	/***************************************************************************************************
	 * 
	 * Server Messages
	 * 
	 **************************************************************************************************/

	/**
	 * JOBN - Send a normal job
	 */
	@ServerCommand(cmd = "JOBN")
	public void S_JOBN(int submitTime, int jobId, int estRuntime, int core, int memory, int disk) {
		// Setup job object
		var j = new Job();
		j.submitTime 	= submitTime;
		j.jobId 		= jobId;
		j.estRunTime 	= estRuntime;
		j.cores 		= core;
		j.memory 		= memory;
		j.disk 			= disk;
		j.state = EnumJobState.Waiting;

		incomingJobs.enqueue(j);
	}

	@ServerCommand(cmd = "JCPL")
	public void S_RecentlyCompletedJobInfo(int endTime, int jobId, String serverType, int serverId) {
		// Update local system state - dequeue the job from the servers working queue
		// and add to completed list.
		var svr = servers.get(serverType).servers.get(serverId);
		var j = svr.jobs.dequeue();

		j.state = EnumJobState.Completed;
		j.endTime = endTime;

		svr.completedJobs.enqueue(j);

		// Set the next job to running.
		if (svr.jobs.peek() != null)
			svr.jobs.peek().state = EnumJobState.Running;

		jobCompleted = true;
		recentlyCompletedJob = j;
		serverWithRecentlyCompletedJob = svr;
	}

	@ServerCommand(cmd = "RESF")
	public void S_NotifyServerFailure(String serverType, int jobId, int timeOfFailure) {

	}

	@ServerCommand(cmd = "RESR")
	public void S_NotifyServerRecovery(String serverType, int jobId, int timeOfRecovery) {

	}

	@ServerCommand(cmd = "CHKQ")
	public void S_CheckQueue() {

	}

	@ServerCommand(cmd = "DATA")
	public void S_Data(int numberOfRecords, int maxRecordLength) {
		C_OK(); // Ack cmd

		if (numberOfRecords > 0) {
			// Read in numberOfRecords that follows the acknowledgment.
			try {
				var array = (Object[]) Array.newInstance(dataRecordType, numberOfRecords);

				for (int i = 0; i < numberOfRecords; i++) {
					array[i] = read(dataRecordType);
				}

				response_data = array;

				C_OK(); // Ack data
			} catch (Exception ex) {
				writeConsoleSysMsg(EnumSysLogLevel.Full, ex.getMessage());
			}
		}
	}

	@ServerCommand(cmd = "OK")
	public void S_OK() {
	}

	@ServerCommand(cmd = "QUIT")
	public void S_QUIT() {
		// Upon receiving a QUIT confirmation from the server, stop the client from
		// looping.
		runClient = false;
	}

	@ServerCommand(cmd = "NONE")
	public void S_None() {
		// Once we receive a NONE message, we can initiate the QUIT sequence.
		C_Quit();
	}

	@ServerCommand(cmd = ".")
	public void S_Dot() {
	}

	@ServerCommand(cmd = "ERR")
	public void S_Error(String message) {
		writeConsoleErrMsg(EnumSysLogLevel.Info, ANSI_RED, "ERR: ", message);
	}

	/***************************************************************************************************
	 * 
	 * Socket Utilities
	 * 
	 **************************************************************************************************/

	private void initSocket() throws UnknownHostException, IOException {
		socket = new Socket("localhost", 50000);

		inputStream = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		outputStream = new DataOutputStream(socket.getOutputStream());
	}

	private void closeSocket() {
		try {
			outputStream.close();
			inputStream.close();
			socket.close();
		} catch (IOException ioex) {
			System.err.println(ioex.getMessage());
		}
	}

	private void write(StringBuilder sb) {
		try {
			outputStream.write(sb.append("\n").toString().getBytes());
			outputStream.flush();

			if (EnumSysLogLevel.Info.isLowerOrEqualTo(sysMessageLogLevel))
				sb.insert(0, "TXD: ").insert(0, ANSI_GREEN).deleteCharAt(sb.length() - 1).append(ANSI_WHITE);
			writeConsoleMsg(EnumSysLogLevel.Info, sb);
		} catch (Exception ex) {

			writeConsoleErrMsg(EnumSysLogLevel.Full, ex.getMessage());
		}
	}

	private void write(Object... args) {
		// Initialise a Stringbuilder with starting size of 64
		StringBuilder sb = new StringBuilder(64).append(args[0]);

		for (int i = 1; i < args.length; i++) {
			sb.append(' ').append(args[i]);

		}

		write(sb);
	}

	private String read() {
		try {
			var response = inputStream.readLine();

			writeConsoleMsg(EnumSysLogLevel.Info, new StringBuilder(response).insert(0, "RXD: "));

			return response;
		} catch (Exception ex) {
			writeConsoleErrMsg(EnumSysLogLevel.Full, ex.getMessage());
			return "";
		}
	}

	private Integer readInt() {
		var res = read();

		if (res.length() > 0) {
			return Integer.valueOf(res);
		}

		return null;
	}

	private <T> T read(Class<T> asType) {
		var response = read().split(" ");
		int responseIdx = 0;

		try {
			// Instantiate a new intance of the desired type. Because Java has god-aweful
			// generics
			// we need to recast that instance as the desired type afterwards..
			// All objects will require a default constructor with no parameters (must be
			// manually
			// specified in type), thus we will always use the first constructor for
			// instantiation.
			T instance = asType.cast(asType.getConstructors()[0].newInstance());

			for (var member : instance.getClass().getFields()) {
				// Break loop if there are no more params.
				// In some cases there will be objects that can store more values should they be
				// provided by the incoming data. If not we will break early and not set those
				// fields.
				if (responseIdx >= response.length)
					break;

				member.set(instance, castArgument(member.getType(), response[responseIdx++]));
			}

			return instance;
		} catch (Exception ex) {
			writeConsoleErrMsg(EnumSysLogLevel.Full, ex.getMessage());
		}

		return null;
	}

	/***************************************************************************************************
	 * 
	 * Utilities
	 * 
	 **************************************************************************************************/
	
	public Job getRecentlyCompletedJob() {
		if (jobCompleted)
			return recentlyCompletedJob;
		else
			return null;
	}
	 
	public ComputeServer getServerWithMostRecentJobCompletion() {
		if (jobCompleted) {
			jobCompleted = false;
			return serverWithRecentlyCompletedJob;
		} else
			return null;
	}
	 
	/**
	 * Find all servers of same type and smaller. (How to determine this??)
	 * Order results by size of waiting queue.
	 */
	public List<ComputeServer> getSmallerServersOrderedByJobQueue(ComputeServer cs) {
		final int WAITING_JOB_THRESHOLD = 1;

		List<ComputeServer> results = new ArrayList<>();
		for (var list : this.servers.values()) {
			for (var svr : list.servers) {
				if (list.order < cs.parentCollection.order && svr.jobs.size() > 1)
					results.add(svr);
				// if (list.order >= cs.parentCollection.order || svr.jobs.size() <= WAITING_JOB_THRESHOLD)
					// continue;
			}
		}
		if (results.size() > 0) {
			 results.sort(
					(a, b) -> {
						if (a.jobs.size() > b.jobs.size())
							return -1;
						else if (a.jobs.size() < b.jobs.size())
							return 1;
						else
							return 0;
				}
			);
			return results;
		}
		return null;
	}


	public <T> T[] getDataResponse() {
		if (response_data != null && response_data.length > 0 && response_data.getClass().getComponentType().isAssignableFrom(dataRecordType)) {
			return (T[]) response_data;
		} else {
			return null;
		}
	}

	public ComputeServer getCachedServerInfo(ServerState state) {
		for (final var cs : getServerInfo().get(state.type).servers) {
			if (cs.server.id != state.id)
				continue;
			return cs;
		}
		return null;
	}

	private StringBuilder paramsArrayToStringBuilder(Object... params) {
		if (params.length == 1)
			return new StringBuilder(params[0].toString());
		else if (params.length == 0) {
			return new StringBuilder();
		} else {
			var sb = new StringBuilder(64);
			for (Object object : params) {
				sb.append(object);
			}

			return sb;
		}
	}

	public void writeConsoleSysMsg(EnumSysLogLevel level, Object... params) {
		writeConsoleMsg(level,
				paramsArrayToStringBuilder(params).insert(0, "SYS: ").insert(0, ANSI_YELLOW).append(ANSI_WHITE));
	}

	public void writeConsoleSysMsg(EnumSysLogLevel level, StringBuilder sb) {
		writeConsoleMsg(level, sb.insert(0, "SYS: ").insert(0, ANSI_YELLOW).append(ANSI_WHITE));
	}

	public void writeConsoleErrMsg(EnumSysLogLevel level, StringBuilder sb) {
		writeConsoleMsg(level, sb.insert(0, "ERR: ").insert(0, ANSI_RED).append(ANSI_WHITE));
	}

	public void writeConsoleErrMsg(EnumSysLogLevel level, String s) {
		writeConsoleMsg(level, new StringBuilder(s).insert(0, "ERR: ").insert(0, ANSI_RED).append(ANSI_WHITE));
	}

	public void writeConsoleErrMsg(EnumSysLogLevel level, Object... args) {
		var sb = paramsArrayToStringBuilder(args);
		writeConsoleMsg(level, sb.insert(0, "ERR: ").insert(0, ANSI_RED).append(ANSI_WHITE));
	}

	public void writeConsoleMsg(EnumSysLogLevel level, StringBuilder sb) {
		if (level.isLowerOrEqualTo(sysMessageLogLevel))
			System.out.println("+ " + sb);
	}

	public Map<String, ServerList> getServerInfo() {
		return this.servers;
	}
}

/***************************************************************************************************
 * 
 * TYPES
 * 
 **************************************************************************************************/

/**
 * ServerCommand annotation type. This is used to mark methods which should
 * be interpreted as commands originating from the server.
 * The cmd field should be the expected command string from the server,
 * i.e. 'JOBN', 'JCPL', etc
 * 
 * The interface must be marked with a Rentention policy annotation to ensure
 * it is retained at runtime.
 */
@Retention(RetentionPolicy.RUNTIME)
@interface ServerCommand {
	String cmd();
}

/**
 * ClientCommand annotation type. This is used to mark methods which should
 * be interpreted as commands originating from the Client.
 * The cmd field should be the expected command string from the Client,
 * i.e. 'SCHD', 'AUTH', etc
 * 
 * The interface must be marked with a Rentention policy annotation to ensure
 * it is retained at runtime.
 */
@Retention(RetentionPolicy.RUNTIME)
@interface ClientCommand {
	String cmd();
}

/*
 * Each of the following types are likely to be used by the read(Class<T>)
 * function.
 * This function will read in responses from a DATA record block and cast the
 * arguments
 * into the class instance fields.
 * It is important that each of the following types have their fields in the
 * same order
 * a data message is going to provide them so that everything is correctly
 * algned and
 * injected.
 */
class Job extends Applicance {
	public int jobId;
	public int queueId;
	public int submitTime;
	public int queuedTime;
	public int estRunTime;

	public EnumJobState state;
	public int endTime;

	public Job() {
	}
}

//jobID jobState submitTime startTime estRunTime core memory disk.
class JobStatus extends Applicance {
	public int jobId;
	public int state;
	public int submitTime;
	public int queuedTime;
	public int estRunTime;

	public JobStatus() {
	}

	public EnumJobState getState() {
		return EnumJobState.getJobState(state);
	}
}

class ServerState {
	public String type;
	public int id;
	public String state;
	public int curStartTime;
	public int core;
	public int memory;
	public int disk;
	public int waitingJobs;
	public int runningJobs;

	// Only if failures simulated
	public int numberOfFailures;
	public int totalFailTime;
	public int meanTimeToFailure;
	public int meanTimeToRecovery;
	public int meanAbsDeviationOfFailure;
	public int lastServerStartTime;

	public ServerState() {
	}
}

class Applicance {
	public int cores;
	public int memory;
	public int disk;

	public Applicance() {
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append(cores)
				.append(' ')
				.append(memory)
				.append(' ')
				.append(disk)
				.toString();
	}
}

enum EnumGETSState {
	All("All"),
	Type("Type"),
	Capable("Capable"),
	Available("Avail");

	private String cmdLbl;

	private EnumGETSState(String label) {
		this.cmdLbl = label;
	}

	public String getLabel() {
		return cmdLbl + " ";
	}
}

enum EnumListType {
	All("*"),
	Number("#"),
	Dollar("$"),
	Index("i");

	private String cmd;

	private EnumListType(String command) {
		this.cmd = command;
	}

	public String getCommand() {
		return cmd;
	}
}

enum EnumJobState {
	Submitted(0),
	Waiting(1),
	Running(2),
	Suspended(3),
	Completed(4),
	PreEmpted(5),
	Failed(6),
	Killed(7),
	INVALID(8);

	private int state;

	private EnumJobState(int s) {
		this.state = s;
	}

	public int getState() {
		return state;
	}

	public static EnumJobState getJobState(int s) {
		if (s >= Submitted.ordinal() && s <= Killed.ordinal())
			return EnumJobState.values()[s];
		else
			return EnumJobState.INVALID;
	}
}

/**
 * Utility enumeration for managing the verbosity of client terminal output.
 */
enum EnumSysLogLevel {
	None(0),
	Sys(3),
	Info(5),
	Full(10);

	private int level;

	private EnumSysLogLevel(int lvl) {
		this.level = lvl;
	}

	public int logLevel() {
		return level;
	}

	public boolean isLowerOrEqualTo(EnumSysLogLevel other) {
		return this.level <= other.level;
	}
}

/**
 * A container for caching a method and it's parameters for dynamic invocation.
 */
class MethodCallData {
	public Method method;
	public Parameter[] parameters;

	public MethodCallData(Method m, Parameter[] p) {
		this.method = m;
		this.parameters = p;
	}
}

class ServerList {
	public final List<ComputeServer> servers;
	public final int order;

	public ServerList(int order, List<ComputeServer> svrs) {
		this.order = order;
		this.servers = svrs;
	}

	public ServerList(int order, ComputeServer firstServer) {
		this.order = order;
		firstServer.setServerList(this);
		this.servers = new ArrayList<>(20);
		servers.add(firstServer);
	}
}

class ComputeServer {
	public final ServerState server;
	public final JobQueue jobs = new JobQueue();
	public final JobQueue completedJobs = new JobQueue();
	public ServerList parentCollection;

	public ComputeServer(ServerState svr) {
		server = svr;
	}

	public ComputeServer(ServerState svr, ServerList parent) {
			server = svr;
			parentCollection = parent;
		}

	public void setServerList(ServerList parent) {
		this.parentCollection = parent;
	}
}

class JobQueue extends LinkedList<Job> {

	public JobQueue() {
		super();
	}

	public void enqueue(Job j) {
		this.add(j);
	}

	public Job dequeue() {
		return this.poll();
	}
}

interface IScheduler {
	void run();
}

abstract class Scheduler implements IScheduler {
	private boolean initialised = false;
	protected MyClient client;

	void initialise(MyClient client) {
		this.client = client;
		initialised = true;
	}

	public boolean initialised() {
		return initialised;
	}
}

class RoundRobinScheduler extends Scheduler {
	private ServerState largestServer;
	private int largestServerCount;
	private int serverScheduleIndex = 0;

	public void initialise(MyClient c) {
		super.initialise(c);
		client.loadServerInfo();
		findFirstLargestServer();
	}

	private void findFirstLargestServer() {
		int cpuMax = Integer.MIN_VALUE;

		// Sort the server types according to the order they were received.
		// We need to do this because larger compute nodes are sent later,
		// and in situations where two types contain the same number of cores
		// there is the chance that they will not be in order within the HashMap
		var sortedList = new ArrayList<>(client.getServerInfo().entrySet());
		Collections.sort(sortedList, (o1, o2) -> {
			var first = (Map.Entry<String, ServerList>) o1;
			var second = (Map.Entry<String, ServerList>) o2;

			if (first.getValue().order > second.getValue().order)
				return 1;

			if (first.getValue().order < second.getValue().order)
				return -1;

			return 0;
		});

		/*
		 * Need to find the first server type with the largest number of cores.
		 * Iterate over the local cached server data, if we find a server with more cpu
		 * resources
		 * than our previous largestServer record AND the types do not match then set
		 * largestServer
		 * to the new candidate.
		 */
		for (var set : sortedList) {
			var serverTypeList = set.getValue().servers;

			if (serverTypeList.size() > 0) {
				var firstServer = serverTypeList.get(0).server;

				if (largestServer == null
						|| firstServer.core > cpuMax && !firstServer.type.equals(largestServer.type)) {
					cpuMax = firstServer.core;
					largestServer = firstServer;
					largestServerCount = serverTypeList.size();
				}
			}
		}
	}

	@Override
	public void run() {
		var job = client.incomingJobs.dequeue();
		if (job != null) {
			client.C_Schedule(job, largestServer.type, serverScheduleIndex++ % largestServerCount);

			client.handleNextMessage(); // Ok message.
		}
	}

}

/**
 * This scheduler works at first by allocating jobs the smallest available server, once all
 * available servers are exhaused it then begins queueing jobs on the a capable server that
 * has the least queued jobs.
 * 
 * The next stage would be responding to job completions, looking for servers that have no more
 * jobs waiting. We would then look at jobs queued on similar or smaller servers, and see if we can
 * reallocate them onto the now free server.
 * 
 * 
 * There could be efforts made to try agressively batch smaller jobs onto much larger servers.
 * Could be done by:
 * 	looking at total available compute on the free server.
 * 	finding the servers with the most scheduled jobs/sorting servers with most scheduled jobs 
 * 	(where server is same size or smaller than us, preferring smaller servers as we can probably
 *  handle more of their jobs at once)
 * 		iterating through those servers and finding how many jobs we can allocate to our free server
 * 			then allocate those jobs.
 */
class Part2Scheduler extends Scheduler {

	public void initialise(MyClient c) {
		super.initialise(c);
		client.loadServerInfo();
	}

	BufferedWriter log = new BufferedWriter(new OutputStreamWriter(System.out));
	final int bufferSize = 4096;
	char[] buffer = new char[bufferSize];
	StringBuilder sb = new StringBuilder(bufferSize);

	long total = 0;
	long iterations = 1;
	long avgWindow = 100;
	long min = Long.MAX_VALUE;
	long max = 0;

	private void printSystemGraph() {
		this.client.C_GetServerState(EnumGETSState.All, null, null);
		this.client.handleNextMessage();
		ServerState[] data = client.getDataResponse();
		client.response_data = null;

		if (data == null || data.length == 0)
			return;

		sb.delete(0, sb.length()); // Clear buffer
		sb.append("\033[H\033[2J").append(String.format("Output AVG:%6sus MIN:%6sus MAX:%6sus\n", total / 1000, min / 1000, max / 1000));
		for (ServerState serverState : data) {
			var cache = this.client.getCachedServerInfo(serverState);

			String bars = "|";
			
			sb.append(String.format(
					MyClient.ANSI_WHITE + "%-10s %-10s %-10s %10s %-10s\n" + MyClient.ANSI_WHITE,
					serverState.type,
					serverState.id,
					serverState.state,
					MyClient.ANSI_GREEN + cache.completedJobs.size(),
					MyClient.ANSI_GREEN + bars.repeat(serverState.runningJobs) +
							MyClient.ANSI_RED + bars.repeat(serverState.waitingJobs)));
		}
		var start = System.nanoTime();
	
		// Copy output to buffer.
		// Write buffer to output
		try {
			if (sb.length() > buffer.length)
				buffer = new char[sb.length()];
			sb.getChars(0, sb.length(), buffer, 0);
			log.write(buffer, 0, sb.length());
			log.flush();
		} catch (Exception ex) {
			// Ignore dont care
		}

		final var time = System.nanoTime() - start; 
		// Rolling average
		total -= total / avgWindow;
		total += time / avgWindow;
		if (time < min)
			min = time;
		else if (time > max)
			max = time;
	}
	
	public void run() {
		// Get the most recent job, and if not null, we will queue it.
		final Job dequeue = this.client.incomingJobs.dequeue();
		if (dequeue != null) {
			// Find all available servers that can handle the job and schedule it on the one 
			// with the fewest waiting jobs
			this.client.C_GetServerState(EnumGETSState.Available, null, dequeue);
			this.client.handleNextMessage(); // OK
			ServerState[] data = client.getDataResponse();
			if (data != null) {
				// Finding server with least running jobs
				ServerState sTarget = null;
				for (final ServerState ss : data) {

					if (sTarget != null) {
						if (ss.runningJobs < sTarget.runningJobs)
							sTarget = ss;
						
					} else {
						sTarget = ss;
					}
				}
				this.client.C_Schedule(dequeue, sTarget.type, sTarget.id);
				this.client.response_data = null; // Clear response data.
			}
			// If there are not available servers, then look for all capable servers, and
			// schedule it on the one with the lease queued jobs
			else {
				this.client.C_GetServerState(EnumGETSState.Capable, (String) null, (Applicance) dequeue);
				this.client.handleNextMessage(); // OK
				data = client.getDataResponse();
				if (data != null) {
					ComputeServer computeServer = null;
					ServerState ss = null;
					// For each of the capable servers, look up in our local cache for the one with
					// smallest job queue.
					for (final ServerState serverState2 : data) {
						if (ss != null) {
							if (serverState2.waitingJobs <= ss.waitingJobs) // Swap with server with least waiting jobs
								ss = serverState2;
						} else {
							ss = serverState2; // Default initial
						}
						/*
						final var serverCache = client.getCachedServerInfo(serverState2);
						if (computeServer != null) {
							if (computeServer.jobs.size() <= serverCache.jobs.size())
								continue;
							computeServer = serverCache;
						} else {
							computeServer = serverCache;
						}
						*/
					}
					// Assign
					this.client.C_Schedule(dequeue, ss.type, ss.id);
					this.client.response_data = null; // Clear response data
				}
			}
		}

		boolean doRebalance = true;
		/*
		 * The next stage would be responding to job completions, looking for servers
		 * that have no more
		 * jobs waiting. We would then look at jobs queued on similar or smaller
		 * servers, and see if we can
		 * reallocate them onto the now free server.
		 */
		var completed = client.getServerWithMostRecentJobCompletion();

		if (doRebalance && completed != null) {
			client.C_ListJobs(completed.server.type, completed.server.id);
			client.handleNextMessage();
			JobStatus[] svrJobs = client.getDataResponse();
			this.client.response_data = null;

			int availCpu = completed.server.core;
			int availMem = completed.server.memory;
			int availDisk = completed.server.disk;

			if (svrJobs != null) {
				for (JobStatus jobStatus : svrJobs) {
					if (jobStatus.getState() == EnumJobState.Running) {
						availCpu -= jobStatus.cores;
						availMem -= jobStatus.memory;
						availDisk -= jobStatus.disk;
					} else if (jobStatus.getState() == EnumJobState.Waiting) {
						return;
					}
				}

				this.client.C_GetServerState(EnumGETSState.All, null, null);
				ServerState[] data = client.getDataResponse();
				var serversToCheck = new ArrayList<ServerState>();
				if (data != null) {
					for (var s : data) {
						// Make sure we are not including the target server
							if (s.waitingJobs > 0) {
								serversToCheck.add(s);
							}
					}
				}
				client.handleNextMessage();

				if (serversToCheck.size() > 0) {
					// Sort servers with most waiting jobs first
					serversToCheck.sort((a, b) -> {
						if (a.waitingJobs > b.waitingJobs)
							return -1;
						else if (a.waitingJobs < b.waitingJobs)
							return 1;
						else
							return 0;
					});

					for (var s : serversToCheck) {

						// The the sample server is smaller or the same size as the completed server we
						// can look at its jobs as they will fit on the completed server.
						if (s.core <= completed.server.core && s.memory <= completed.server.memory
								&& s.disk <= completed.server.disk) {

							client.C_ListJobs(s.type, s.id);
							client.handleNextMessage(); //
							JobStatus[] js = client.getDataResponse();
							if (js != null && js.length > 0) {
								for (var job : js) {
									if (job.getState() == EnumJobState.Waiting) {
										if (availCpu - job.cores >= 0 &&
												availMem - job.memory >= 0 &&
												availDisk - job.disk >= 0) {
											client.C_MigrateJob(job.jobId, s.type, s.id, completed.server.type,
													completed.server.id);
											availCpu -= job.cores;
											availMem -= job.memory;
											availDisk -= job.disk;
										}
									}
								}
							}
							// Clear data
							client.response_data = null;
						}
					}
				}
			}
		}
		
		// printSystemGraph();
	}
}

class Pair<T, U> {
	public T first;
	public U second;

	public Pair(T f, U s) {
		first = f;
		second = s;
	}
}