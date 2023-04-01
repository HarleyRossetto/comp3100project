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

	private SysLogLevel sysMessageLogLevel = SysLogLevel.Info;

	private Map<String, MethodCallData> commandMap = new HashMap<String, MethodCallData>();

	private boolean runClient = false;

	// IO Objects
	private Socket socket;
	private BufferedReader inputStream;
	private DataOutputStream outputStream;

	private Class<?> dataRecordType = null;

	private Object[] response_data = null;

	private boolean jobReceived = false;
	private Job recentJob = null;
	private boolean jobCompleted = false;

	// List of servers, keeps track of servers and jobs queued on them.
	private Map<String, ServerList> servers;

	private Scheduler scheduler;

	public MyClient() {
		mapCommands();

		try {
			initSocket();
			runClient = true;
		} catch (Exception ex) {
			runClient = false;
		}

		if (runClient) {
			scheduler = new RoundRobinScheduler();

			C_AuthHandshake();

			// Initial ready message
			while (runClient) {
				LRR();
			}
		}
			
		closeSocket();
	}

	private void LRR() {
		C_Ready();

		handleNextMessage(); // Typically JOBN, JCPL, NONE

		if (!scheduler.initialised())
			scheduler.initialise(this);
		
		if (jobReceived && recentJob != null) {
			jobReceived = false;

			// Run the scheduler with most recent job.
			scheduler.run(recentJob);

			recentJob = null;
		} else if (jobCompleted) {
			// If a job has been completed (rxd JCPL) then we dont have any response so just go back to REDY
			jobCompleted = false;
		} else { // We are quitting, so read that in,
			handleNextMessage(); // Quit
		}
	}

	public void loadServerInfo() {
		C_GetServerState(GETS_State.All, null, null);
		
		handleNextMessage(); // Hopefully DATA

		// Assume there are at least 3 servers per type, 
		// That way we don't allocate too much or too little space causing reallocations and copies.
		servers = new HashMap<String, ServerList>(response_data.length / 3);

		var svrs = (ServerState[])response_data;

		int order = 0;
		for (ServerState serverState : svrs) {
			// If we already have this type registered, add this server to the array.
			// otherwise add new type mapping and server to dictionary.
			var s = servers.get(serverState.type);
			if (s != null) {
				s.servers.add(new Server_ABC(serverState));
				//servers.put(serverState.type, new ServerList(0, new Server_ABC(serverState)));
			} else {
				servers.put(serverState.type, new ServerList(order, new Server_ABC(serverState)));
				order++;
				//servers.put(serverState.type, new ArrayList<Server_ABC>(List.of(new Server_ABC(serverState))));
			}
		}

		handleNextMessage(); // .
	}

	/**
	 * Used to cache the expected object type that will be read in by the next DATA message
	 * @param type
	 */
	public void setDataRecordType(Class<?> type) {
		this.dataRecordType = type;
	}

/***************************************************************************************************
 * 
 * 								Command Discover & Execution
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
				writeConsoleSysMsg(SysLogLevel.Full, "Mapped command ", cmdat.cmd());
				count++;
			}
		}

		writeConsoleSysMsg(SysLogLevel.Info, new StringBuilder().append("Mapped ").append(count).append(" commands."));
	}

	private void findAndExecuteCommand(String args) {
		final int CMD_IDX = 0;
		var splitArgs = args.split(" ");

		// Try get the command, if it doesn't exist print error and return.
		var commandMethod = commandMap.get(splitArgs[CMD_IDX]);
		if (commandMethod == null) {
			writeConsoleErrMsg(SysLogLevel.Full, "Command ", splitArgs[CMD_IDX], " is invalid.");
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
			writeConsoleSysMsg(SysLogLevel.Full, 
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
				writeConsoleErrMsg(SysLogLevel.Full, "Cannot cast param ", p.getName(), " of type ", pType);
			} else {
				castParams.add(castType);
			}

			sb.append("\t").append(p.getName()).append(": ").append(castType).append("\n");
		}

		try {
			writeConsoleSysMsg(SysLogLevel.Full, new StringBuilder("Invoking: ").append(m.method.getName()));
			// If we have some arguments, then print them out if running with SysLogLevel.Full verbosity.
			if (sb.length() > 0)
				writeConsoleSysMsg(SysLogLevel.Full, sb);
			// Invoke the method with params
			m.method.invoke(this, castParams.toArray());
		} catch (Exception ex) {
			writeConsoleSysMsg(SysLogLevel.Full,
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
 * 											Client Messages
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
	public void C_GetServerState(GETS_State state, String type, Applicance sys) {
		var sb = new StringBuilder("GETS ").append(state.getLabel());
		switch (state) {
			case All:
				break;
			case Type:
				if (type == null) {
					writeConsoleSysMsg(SysLogLevel.Full, "GETS requires param type to be specified with flag Type");
					return;
				}
				sb.append(type);
				break;
			case Capable:
			case Available:
				if (sys == null) {
					writeConsoleSysMsg(SysLogLevel.Full, "GETS requires param sys to be specified with flag ", state.getLabel());
					return;
				}
				sb.append(sys.toString());
				break;
		}
		setDataRecordType(ServerState.class);
		write(sb);
	}

	/**
	 * The SCHD command schedules a job (jobID) to the server (serverID) of
	 * serverType.
	 * {@code SCHD 3 joon 1}
	 */
	@ClientCommand(cmd = "SCHD")
	public void C_Schedule(Job job, String serverType, int serverId) {
		var svr = servers.get(serverType).servers.get(serverId);
		
		// If the server currently has no jobs in its queue, we will default the job state to running.
		if (svr.jobs.isEmpty()) 
			job.state = EnumJobState.Running;
		else // Otherwise the state will be submitted.
			job.state = EnumJobState.Submitted;

		svr.jobs.enqueue(job);
		
		write("SCHD", job.jobId, serverType, serverId);	
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
	public void C_GetJobInfo(String queueName, ListType type, Integer i) {
		var sb = new StringBuilder("LSTQ ").append(queueName).append(' ');

		if (type == ListType.Index) {
			if (i == null) {
				writeConsoleErrMsg(SysLogLevel.Full, "Cannot get job info for queue: ", queueName, " with Index type when i is null");
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

		// Now a data response..
		// setDataRecordType(JobState.class);
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
		// runClient = false;
	}

	@ClientCommand(cmd = "OK")
	public void C_OK() {
		write("OK");
	}

/***************************************************************************************************
 * 
 * 											Server Messages
 * 
 **************************************************************************************************/

	/**
	 * JOBN - Send a normal job
	 */
	@ServerCommand(cmd = "JOBN")
	public void S_JOBN(int submitTime, int jobId, int estRuntime, int core, int memory, int disk) {
		// Setup job object
		recentJob = new Job();
		recentJob.submitTime 	= submitTime;
		recentJob.jobId 		= jobId;
		recentJob.estRunTime 	= estRuntime;
		recentJob.cores 		= core;
		recentJob.memory 		= memory;
		recentJob.disk 			= disk;
		recentJob.state 		= EnumJobState.Waiting;

		jobReceived = true;
	}

	@ServerCommand(cmd = "JCPL")
	public void S_RecentlyCompletedJobInfo(int endTime, int jobId, String serverType, int serverId) {
		// Update local system state - dequeue the job from the servers working queue and add to completed list.
		var svr = servers.get(serverType).servers.get(serverId);
		var j = svr.jobs.dequeue();
		
		j.state = EnumJobState.Completed;
		j.endTime = endTime;
		
		svr.completedJobs.enqueue(j);

		// Set the next job to running.
		if (svr.jobs.peek() != null)
			svr.jobs.peek().state = EnumJobState.Running;

		jobCompleted = true;
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
	public void S_Data(int numberOfRecords, int recordLength) {
		C_OK(); // Ack cmd

		// Read in numberOfRecords that follows the acknowledgment.
		try {
			var array = (Object[]) Array.newInstance(dataRecordType, numberOfRecords);

			for (int i = 0; i < numberOfRecords; i++) {
				array[i] = read(dataRecordType);
			}

			response_data = array;

			C_OK(); // Ack data
		} catch (Exception ex) {
			writeConsoleSysMsg(SysLogLevel.Full, ex.getMessage());
		}
	}

	@ServerCommand(cmd = "OK")
	public void S_OK() { }
	
	@ServerCommand(cmd = "QUIT")
	public void S_QUIT() {
		//Upon receiving a QUIT confirmation from the server, stop the client from looping.
		runClient = false;
	}

	@ServerCommand(cmd = "NONE")
	public void S_None() {
		// Once we receive a NONE message, we can initiate the QUIT sequence.
		C_Quit();
	}

	@ServerCommand(cmd = ".")
	public void S_Dot() { }

	@ServerCommand(cmd = "ERR")
	public void S_Error(String message) {
		writeConsoleErrMsg(SysLogLevel.Info, ANSI_RED, "ERR: ", message);
	}
		
/***************************************************************************************************
 * 
 * 										Socket Utilities
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
			if (SysLogLevel.Info.isLowerOrEqualTo(sysMessageLogLevel))
				sb.insert(0, "TXD: ").insert(0, ANSI_GREEN).deleteCharAt(sb.length() - 1).append(ANSI_WHITE);
				writeConsoleMsg(SysLogLevel.Info, sb);
			} catch (Exception ex) {
			
			writeConsoleErrMsg(SysLogLevel.Full, ex.getMessage());
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
			
			writeConsoleMsg(SysLogLevel.Info, new StringBuilder(response).insert(0, "RXD: "));

			return response;
		} catch (Exception ex) {
			writeConsoleErrMsg(SysLogLevel.Full, ex.getMessage());
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
			// Instantiate a new intance of the desired type. Because Java has god-aweful generics
			// we need to recast that instance as the desired type afterwards..
			// All objects will require a default constructor with no parameters (must be manually
			// specified in type), thus we will always use the first constructor for instantiation.
			T instance = asType.cast(asType.getConstructors()[0].newInstance());

			for (var member : instance.getClass().getFields()) {
				// Break loop if there are no more params.
				// In some cases there will be objects that can store more values should they be
				// provided by the incoming data. If not we will break early and not set those fields.
				if (responseIdx >= response.length)
					break;

				member.set(instance, castArgument(member.getType(), response[responseIdx++]));
			}

			return instance;
		} catch (Exception ex) {
			writeConsoleErrMsg(SysLogLevel.Full, ex.getMessage());
		}

		return null;
	}

/***************************************************************************************************
 * 
 * 												Utilities
 * 
 **************************************************************************************************/

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

	private void writeConsoleSysMsg(SysLogLevel level, Object... params) {
		writeConsoleMsg(level, paramsArrayToStringBuilder(params).insert(0, "SYS: ").insert(0, ANSI_YELLOW).append(ANSI_WHITE));
	}

	private void writeConsoleSysMsg(SysLogLevel level, StringBuilder sb) {
		writeConsoleMsg(level, sb.insert(0, "SYS: ").insert(0, ANSI_YELLOW).append(ANSI_WHITE));
	}

	private void writeConsoleErrMsg(SysLogLevel level, StringBuilder sb) {
		writeConsoleMsg(level, sb.insert(0, "ERR: ").insert(0, ANSI_RED).append(ANSI_WHITE));
	}

	private void writeConsoleErrMsg(SysLogLevel level, String s) {
		writeConsoleMsg(level, new StringBuilder(s).insert(0, "ERR: ").insert(0, ANSI_RED).append(ANSI_WHITE));
	}

	private void writeConsoleErrMsg(SysLogLevel level, Object... args) {
		var sb = paramsArrayToStringBuilder(args);
		writeConsoleMsg(level, sb.insert(0, "ERR: ").insert(0, ANSI_RED).append(ANSI_WHITE));
	}

	private void writeConsoleMsg(SysLogLevel level, StringBuilder sb) {
		if (level.isLowerOrEqualTo(sysMessageLogLevel))
			System.out.println("+ " + sb);
	}


	public Map<String, ServerList> getServerInfo() {
		return this.servers;
	}
}

/***************************************************************************************************
 * 
 * 												TYPES
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

class JobState extends Applicance {
	public int id;
	public int state;
	public int submitTime;
	public int startTime;
	public int estRunTime;

	public EnumJobState getStateEnum() {
		return EnumJobState.getJobState(state);
	}
}

class Job extends Applicance {
	public int jobId;
	public int queueId;
	public int submitTime;
	public int queuedTime;
	public int estRunTime;

	public EnumJobState state;
	public int endTime;

	public Job() { }
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

	public ServerState() { }
}

class Server extends Applicance {
	public String type;
	public int limit;
	public String state;
	public int bootupTime;
	public float hourlyRate;

	public Server() { }
}

class Applicance {
	public int cores;
	public int memory;
	public int disk;

	public Applicance() { }

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

enum GETS_State {
	All("All"),
	Type("Type"),
	Capable("Capable"),
	Available("Avail");

	private String cmdLbl;

	private GETS_State(String label) {
		this.cmdLbl = label;
	}

	public String getLabel() {
		return cmdLbl + " ";
	}
}

enum ListType {
	All("*"),
	Number("#"),
	Dollar("$"),
	Index("i");

	private String cmd;

	private ListType(String command) {
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
enum SysLogLevel {
	None(0),
	Info(5),
	Full(10);

	private int level;

	private SysLogLevel(int lvl) {
		this.level = lvl;
	}

	public int logLevel() {
		return level;
	}

	public boolean isLowerOrEqualTo(SysLogLevel other) {
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
	public final List<Server_ABC> servers;
	public final int order;

	public ServerList(int order, List<Server_ABC> svrs) {
		this.order = order;
		this.servers = svrs;
	}

	public ServerList(int order, Server_ABC firstServer) {
		this.order = order;
		this.servers = new ArrayList<>(20);
	}
}

/*
 
 * 		Server Dictionary
 * 			Servers[] server count, ID is index
 * 				Server info
 * 				Queue
 * 					Job
 * 
 */

class Server_ABC {
	public final ServerState server;
	public final JobQueue jobs = new JobQueue();
	public final JobQueue completedJobs = new JobQueue();

	public Server_ABC(ServerState svr) {
		server = svr;
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
	void run(Job newJob);
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
	public void run(Job newJob) {
		client.C_Schedule(newJob, largestServer.type, serverScheduleIndex++ % largestServerCount);

		client.handleNextMessage(); // Ok message.
	}
}