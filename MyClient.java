import java.io.*;
import java.net.*;
import java.lang.reflect.*;
import java.util.*;
import java.lang.annotation.*;

public class MyClient {
	public static final String ANSI_RESET 	= "\u001B[0m";
	public static final String ANSI_BLACK 	= "\u001B[30m";
	public static final String ANSI_RED 	= "\u001B[31m";
	public static final String ANSI_GREEN 	= "\u001B[32m";
	public static final String ANSI_YELLOW 	= "\u001B[33m";
	public static final String ANSI_BLUE 	= "\u001B[34m";
	public static final String ANSI_PURPLE 	= "\u001B[35m";
	public static final String ANSI_CYAN 	= "\u001B[36m";
	public static final String ANSI_WHITE 	= "\u001B[37m";

	private SysLogLevel sysMessageLogLevel = SysLogLevel.None;

	public static void main(String args[]) {
		new MyClient();
	}

	private Map<String, Method> commandMap = new HashMap<String, Method>();
	private boolean runClient = false;

	private Socket socket;
	private BufferedReader inputStream;
	private DataOutputStream outputStream;

	private Class<?> dataRecordType = null;

	private Object[] server_data = null;

	public MyClient() {
		mapCommands();

		try {
			initSocket();
			runClient = true;
		} catch (Exception ex) {
			runClient = false;
		}

		runClient();

		closeSocket();
	}

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

	private void writeSysMsg(SysLogLevel level, String msg) {
		if (level.isLowerOrEqualTo(sysMessageLogLevel))
			System.out.println(ANSI_YELLOW + "SYS: " + msg + ANSI_WHITE);
	}

	private void runClient() {
		if (runClient) {
			write("HELO");
			read();
			write("AUTH " + System.getProperty("user.name"));
			read();
		}

		while (runClient) {
			LRR();
			//findAndExecuteCommand(read());
		}
	}

	private ServerState[] _servers = null;
	private boolean largestServerFound = false;
	private ServerState largestServer = null;
	private int numLargestServers = 0;
	private boolean jobReceived = false;
	private Job recentJob = null;
	private boolean jobCompleted = false;

	private int bigServerIndex = 0;

	private void LRR() {
		C_Ready();

		readAndExecuteCommand(); //Typically JOBN, JCPL, NONE

		if (!largestServerFound) {
			C_GetServerState(GETS_State.All, null, null);

			readAndExecuteCommand(); // Hopefully DATA

			_servers = (ServerState[]) server_data;
			int cpuMax = 0;
			for (int i = _servers.length - 1; i > 0; i--) {
				if (_servers[i].core > cpuMax) {
					cpuMax = _servers[i].core;
					largestServer = _servers[i];
					numLargestServers = 1;
				} else if (_servers[i].core == cpuMax) {
					numLargestServers++;
				} else {
					continue;
				}
			}
			largestServerFound = true;
			readAndExecuteCommand(); // Receive .
		}
		
		if (jobReceived && recentJob != null) {
			jobReceived = false;
			C_Schedule(recentJob.jobId, largestServer.type, (bigServerIndex++ % numLargestServers));
			
			readAndExecuteCommand(); // Hopefully OK
			
			recentJob = null;
		} else if (jobCompleted) {
			jobCompleted = false;
		} else {
			readAndExecuteCommand(); // Receive .
		}
		
	}

	private void write(StringBuilder sb) {    
		write(sb.toString());
	}

	private void write(String message) {
		try {
			outputStream.write((message + "\n").getBytes());
			outputStream.flush();
			System.out.println(ANSI_GREEN + "TXD: " + message + ANSI_WHITE);
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
		}
	}

	private void write(Object... args) {
		StringBuilder sb = new StringBuilder(32);
		for (Object object : args) {
			sb.append(object).append(' ');
		}
		write(sb);
	}

	private String read() {
		try {
			var response = inputStream.readLine();
			System.out.println("RXD: " + response);
			return response;
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
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
			T instance =  asType.cast(asType.getConstructors()[0].newInstance());
			
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
			System.err.println(ex.getMessage());
		}
		
		return null;
	}

	public void setDataRecordType(Class<?> type) {
		this.dataRecordType = type;
	}

	/**
	 * Creates a map of all methods annotated with the Command annotation type.
	 */
	private void mapCommands() {
		final Class<ServerCommand> commandAnnotationClass = ServerCommand.class;
		int count = 0;

		for (Method m : this.getClass().getDeclaredMethods()) {

			// System.out.println("Method " + m.getName() + " has " + m.getAnnotations().length + " annotations");

			var cmdat = m.getAnnotation(commandAnnotationClass);

			if (cmdat != null) {
				commandMap.put(cmdat.cmd(), m);
				count++;
			}
		}


		writeSysMsg(SysLogLevel.Info, "Mapped " + count + " commands.");
	}

	private void readAndExecuteCommand() {
		findAndExecuteCommand(read());
	}

	private void findAndExecuteCommand(String args) {
		final int CMD_IDX = 0;
		var splitArgs = args.split(" ");

		// Try get the command, if it doesn't exist print error and return.
		var commandMethod = commandMap.get(splitArgs[CMD_IDX]);
		if (commandMethod == null) {
			System.err.println("Command " + splitArgs[CMD_IDX] + " is invalid.");
			return;
		}

		// Prepare arguments for execution
		String[] argsToPass = new String[splitArgs.length - 1];

		// Copy args minus cmd
		for (int i = 1; i <= splitArgs.length - 1; i++)
			argsToPass[i - 1] = splitArgs[i];

		executeCommand(commandMethod, argsToPass);
	}

	/*
	 * Get params
	 * very args and params length matches
	 * cast all args to param types
	 * invoke method with typecase args
	 */
	private void executeCommand(Method m, String[] args) {
		var params = m.getParameters();
		// If we didnt receive enough params, return and print error.
		if (args.length != params.length) {
			System.err.println(
					"Cannot invoke " + m.getName() + ", expected " + params.length + " parameters, got " + args.length);
			return;
		}

		List<Object> castParams = new ArrayList<Object>(params.length);
		int argIdx = 0;
		var sb = new StringBuilder();

		for (Parameter p : params) {
			var pType = p.getType();

			var castType = castArgument(pType, args[argIdx++]);
			if (castType == null) {
				System.err.println("Cannot cast param " + p.getName() + " of type " + pType);
			} else {
				castParams.add(castType);
			}

			sb.append("\t").append(p.getName()).append(": ").append(castType).append("\n");
		}

		try {
			writeSysMsg(SysLogLevel.Info, "Invoking: " + m.getName());
			writeSysMsg(SysLogLevel.Full, sb.toString());
			// Invoke the method with params
			m.invoke(this, castParams.toArray());
		} catch (Exception ex) {
			System.err.println("Fail to invoke " + m.getName() + "\n" + ex.getMessage());
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


	/**
	 * 
	 * 	Client Commands
	 * 
	 */
	@ClientCommand(cmd = "REDY")
	public void C_Ready() {
		write("REDY");
	}

	/**
	 * The GETS command queries server state information at the current simulation time. The All option requests the information on all servers regardless of their state including inactive and unavailable. The Type
	 * option requests the information on servers of a specified type (serverType) regardless of their state, too. The
	 * Capable and Avail options make requests for server state information based on initial resource capacity and
	 * the current resource availability, respectively. For instance, GETS Capable 3 500 1000 and GETS Avail 3 500
	 * 1000 are different in that the response to the former is all servers that can “eventually” provide 3 cores, 500MB
	 * of memory and 1000MB of disk regardless of their current availability. Meanwhile, the response to the latter
	 * is all servers that can “immediately” provide 3 cores, 500MB of memory and 1000MB of disk. With the Avail
	 * option, if there are insufficient available resources and/or waiting jobs, the server is not available for the job.
	 * In general, it is recommended to use the Capable option than the Avail option as the system is often busy,
	 * i.e., all servers are running one or more jobs at any given point in time. In the case of no servers are available
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
					System.err.println("GETS requires param type to be specified with flag Type");
					return;
				}
				sb.append(type);
				break;
			case Capable:
			case Available:
				if (sys == null) {
					System.err.println("GETS requires param sys to be specified with flag " + state.getLabel());
					return;
				}
				sb.append(sys.toString());
				break;
		}
		setDataRecordType(ServerState.class);
		write(sb);
	}

	/**
	 * The SCHD command schedules a job (jobID) to the server (serverID) of serverType.
	 * {@code SCHD 3 joon 1}
	 */
	@ClientCommand(cmd = "SCHD")	 
	public void C_Schedule(int jobId, String serverType, int serverId) {
		write("SCHD", jobId, serverType, serverId);
	}
	
	/**
	 * The ENQJ command places the current job to a specified queue. The name used for the global queue is GQ.
	 * {@code ENQJ GQ}
	 * @param queueName Queue to add current job to.
	 */
	@ClientCommand(cmd = "ENQJ")	 
	public void C_EnqueueJob(String queueName) {
		write("ENQJ", queueName);
	}

	/**
	 * The DEQJ command gets the job at the specified queue position (qID) from the specified queue.
	 * {@code DEQJ GQ 2 // job at the third place (qID of 2 starting from 0) of the global queue}
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
	 * @param queueName Queue to get info for
	 * @param type Index | Number | All | $ (Not used)
	 * @param i Used when Index is specified.
	 */
	@ClientCommand(cmd = "LSTQ")	 
	public void C_GetJobInfo(String queueName, ListType type, Integer i) {
		var sb = new StringBuilder("LSTQ ").append(queueName).append(' ');

		if (type == ListType.Index) {
			if (i == null) {
				System.err.println("Cannot get job info for queue: " + queueName + " with Index type when i is null");
				return;
			}

			sb.append(i.intValue());
		} else {
			sb.append(type.getCommand());
		}
		
		write(sb);

		// Handle responses because of course they have no header and have different formats...
		switch (type) {
			case Index:
				Job response = read(Job.class);
				break;
			case Number:
				var numberOfJobsInQueue = Integer.valueOf(read());
				
				if (numberOfJobsInQueue.intValue() == 0) {
					C_Quit();
				}
				
				break;
			case All:
				setDataRecordType(Job.class);
				break;
			case Dollar: // Currently unused
				break;
		}

	}

	/**
	 * The CNTJ command queries the number of jobs of a specified state, on a specified server. 
	 * The job state is specified by one of state codes, except 0 for ‘submitted’.
	 * {@code
	 * CNTJ joon 0 2 // query the number of running jobs on joon 0
	 * 1 // the response from ds-server, i.e., 1 running job on joon 0
	 * }
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
	 * The EJWT command queries the sum of estimated waiting times on a given server. It does not take into
	 * account the remaining runtime of running jobs. Note that the calculation should not be considered to be
	 * accurate because (1) it is based on estimated runtimes of waiting jobs and (2) more importantly, it does not
	 * consider the possibility of parallel execution of waiting jobs.
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
	 * The LSTJ command queries the list of running and waiting jobs on a given server. The response to LSTJ
	 * is formatted in jobID jobState submitTime startTime estRunTime core memory disk. The job state is sent as
	 * a state code either 1 or 2 for waiting and running, respectively. The response will be a sequence of DATA, a
	 * series of job information and OK message pairs and ‘.’.
	 * @param serverType
	 * @param serverId
	 */
	@ClientCommand(cmd = "LSTJ")	 
	public void C_ListJobs(String serverType, int serverId) {
		write("LSTJ", serverType, serverId);

		// Now a data response..
		//setDataRecordType(JobState.class);
	}
	
	/**
	 * The MIGJ command migrates a job specified by jobID on srcServerID of srcServerType to tgtServerID of
	 * tgtServerType. The job can be of waiting, running or suspended. The successful migration results in the same
	 * behaviour of normal scheduling action. In particular, the job’s state on the target server is determined by
	 * the common criteria of job execution, such as the resource availability and running/waiting jobs of the target
	 * server. The migrated job will “restart” on the target server.
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

		//RX OK
		readAndExecuteCommand();
	}
	
	/**
	 * The KILJ command kills a job. The killed job is pushed back to the queue with the killed time as a new
	 * submission time. The job will be resubmitted with JOBP.
	 * @param serverType
	 * @param serverId
	 * @param jobId
	 */
	@ClientCommand(cmd = "KILJ")	 
	public void C_KillJob(String serverType, int serverId, int jobId) {
		write("KILJ", serverType, serverId, jobId);

		// RX OK
		readAndExecuteCommand();
	}

	/**
	 * The TERM command terminates a server. All waiting/running jobs are killed and re-submitted 
	 * for scheduling with JOBP. The server is then put into the inactive state.
	 * @param serverType
	 * @param serverId
	 */
	@ClientCommand(cmd = "TERM")	 
	public void C_TerminateServer(String serverType, int serverId) {
		write("TERM", serverType, serverId);

		//Read bynber if jobs killed
		var jobsKilled = readInt();
	}
	
	@ClientCommand(cmd = "QUIT")	 
	public void C_Quit() {
		write("QUIT");
	}

	@ClientCommand(cmd = "OK")	 
	public void C_OK() {
		write("OK");
	}

	/**
	 * 
	 * 	Server Commands
	 * 
	 */


	/**
	 * JOBN - Send a normal job
	 */
	@ServerCommand(cmd = "JOBN")
	public void S_JOBN(int submitTime, int jobId, int estRuntime, int core, int memory, int disk) {
		// do some stuff
		recentJob = new Job();
		recentJob.submitTime = submitTime;
		recentJob.jobId = jobId;
		recentJob.estRunTime = estRuntime;
		recentJob.cores = core;
		recentJob.memory = memory;
		recentJob.disk = disk;

		jobReceived = true;
		// Debug!
		//write("QUIT");
		// C_GetServerState(GETS_State.All, null, null);
	}

	@ServerCommand(cmd = "JCPL")
	public void S_RecentlyCompletedJobInfo(int endTime, int jobId, String serverType, int serverId) {
		jobCompleted = true;

		// for (var srv : _servers) {
		// 	if (srv.type == serverType && srv.id == serverId) {
		// 		srv.state = 
		// 	}
		// }
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

		//TODO Need to swap types read depending on context.
		// Read in numberOfRecords that follows the acknowledgment.
		try {
			var array = (Object[])Array.newInstance(dataRecordType, numberOfRecords);

			for (int i = 0; i < numberOfRecords; i++) {
				array[i] = read(dataRecordType);
			}

			server_data = array;

			C_OK(); // Ack data
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
		}
	}
	
	@ServerCommand(cmd = "OK")
	public void S_OK() {
	
	}

	@ServerCommand(cmd = "QUIT")
	public void S_QUIT() {
		runClient = false;
	}

	@ServerCommand(cmd = "NONE")
	public void S_None() {
		C_Quit();
	}

	@ServerCommand(cmd = ".")
	public void S_Dot() {
	}

	@ServerCommand(cmd = "err")
	public void S_Error(String message) {
		System.out.println(ANSI_RED + "ERR: " + message);
	}
}

@Retention(RetentionPolicy.RUNTIME)
@interface ServerCommand {
	String cmd();
}

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

	public Job() {
	}
	// state ?
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

	public ServerState() {}
}

class Server extends Applicance {
	public String type;
	public int limit;
	public String state;
	public int bootupTime;
	public float hourlyRate;

	public Server() {}
}

class Applicance {
	public int cores;
	public int memory;
	public int disk;

	public Applicance() {}

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