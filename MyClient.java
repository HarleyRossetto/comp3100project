import java.io.*;
import java.net.*;
import java.lang.reflect.*;
import java.util.*;
import java.lang.annotation.*;

public class MyClient {
	public static final String ANSI_RESET = "\u001B[0m";
	public static final String ANSI_BLACK = "\u001B[30m";
	public static final String ANSI_RED = "\u001B[31m";
	public static final String ANSI_GREEN = "\u001B[32m";
	public static final String ANSI_YELLOW = "\u001B[33m";
	public static final String ANSI_BLUE = "\u001B[34m";
	public static final String ANSI_PURPLE = "\u001B[35m";
	public static final String ANSI_CYAN = "\u001B[36m";
	public static final String ANSI_WHITE = "\u001B[37m";

	public static void main(String args[]) {
		new MyClient();
		/*
		 * try {
		 * Socket s = new Socket("localhost", 50000);
		 * BufferedReader inputStream = new BufferedReader(new
		 * InputStreamReader(s.getInputStream()));
		 * DataOutputStream outputStream = new DataOutputStream(s.getOutputStream());
		 * 
		 * outputStream.write(("HELO\n").getBytes());
		 * outputStream.flush();
		 * System.out.println("Client: HELO");
		 * 
		 * System.out.println("Server Says: " + inputStream.readLine());
		 * 
		 * outputStream.write(("AUTH " + System.getProperty("user.name") +
		 * "\n").getBytes());
		 * outputStream.flush();
		 * System.out.println("Client: " + "AUTH " + System.getProperty("user.name") );
		 * 
		 * System.out.println("Server Says: " + inputStream.readLine());
		 * 
		 * outputStream.write(("REDY\n").getBytes());
		 * outputStream.flush();
		 * 
		 * System.out.println("Server Says: " + inputStream.readLine());
		 * 
		 * outputStream.write(("QUIT\n").getBytes());
		 * outputStream.flush();
		 * System.out.println("Client: QUIT");
		 * 
		 * System.out.println("Server Says: " + inputStream.readLine());
		 * 
		 * outputStream.close();
		 * inputStream.close();
		 * s.close();
		 * 
		 * } catch (Exception ex) {
		 * System.out.println(ex.toString());
		 * }
		 */
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

	private void writeSysMsg(String msg) {
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

	private boolean largestServerFound = false;
	private ServerState largestServer = null;
	private int numLargestServers = 0;
	private boolean jobReceived = false;
	private Job recentJob = null;

	private void LRR() {
		C_Ready();
		findAndExecuteCommand(read()); // JOBN, JCPL, NONE
		if (!largestServerFound) {
			C_GetServerState(GETS_State.All, null, null);
			findAndExecuteCommand(read()); // Hopefully DATA
			var servers = (ServerState[]) server_data;
			int cpuMax = 0;
			for (int i = servers.length - 1; i > 0; i--) {
				if (servers[i].core > cpuMax) {
					cpuMax = servers[i].core;
					largestServer = servers[i];
					numLargestServers = 1;
				} else if (servers[i].core == cpuMax) {
					numLargestServers++;
				} else {
					continue;
				}
			}
			largestServerFound = true;
			findAndExecuteCommand(read()); // Receive .
		}
		
		if (jobReceived) {
			jobReceived = false;
			C_Schedule(recentJob.jobId, largestServer.type, 0);
			
			findAndExecuteCommand(read()); // Hopefully OK
			
			recentJob = null;
		} else {
			findAndExecuteCommand(read()); // Receive .
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

	private <T> T read(Class<T> asType) {
		var response = read().split(" ");
		int responseIdx = 0;

		try {
			T instance = (T) asType.getConstructors()[0].newInstance();
			
			for (var member : instance.getClass().getFields()) {
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

		writeSysMsg("Mapped " + count + " commands.");
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
			writeSysMsg("Invoking: " + m.getName());
			System.out.print(sb.toString());
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

	@ClientCommand(cmd = "SCHD")	 
	public void C_Schedule(int jobId, String serverType, int serverId) {
		write(new StringBuilder("SCHD ")
			.append(jobId).append(' ')
			.append(serverType)
			.append(' ')
			.append(serverId));
	}
	
	@ClientCommand(cmd = "ENQJ")	 
	public void C_EnqueueJob(String queueName) {
		write(new StringBuilder("ENQJ ").append(queueName));
	}

	@ClientCommand(cmd = "DEQJ")	 
	public void C_DequeueJob(String queueName, int queuePosition) {
		write(new StringBuilder("DEQJ ").append(queueName));
	}
	
	@ClientCommand(cmd = "LSTQ")	 
	public void C_GetJobInfo(String queueName, ListType type, Integer i) {
		var sb = new StringBuilder("LSTQ ").append(queueName).append(' ');

		if (type == ListType.Index) {
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

	@ClientCommand(cmd = "CNTJ")	 
	public void C_JobCountForServer() {

	}
	
	@ClientCommand(cmd = "EJWT")	 
	public void C_EstimatedJobWaitingTime() {

	}
	
	@ClientCommand(cmd = "LSTJ")	 
	public void C_ListJobs() {

	}
	
	@ClientCommand(cmd = "MIGJ")	 
	public void C_MigrateJob() {

	}
	
	@ClientCommand(cmd = "KILJ")	 
	public void C_KillJob() {
	
	}

	@ClientCommand(cmd = "TERM")	 
	public void C_TerminateServer() {
		
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
		var job = new Job();
		job.submitTime = submitTime;
		job.jobId = jobId;
		job.estRunTime = estRuntime;
		job.cores = core;
		job.memory = memory;
		job.disk = disk;

		recentJob = job;
		jobReceived = true;
		// Debug!
		//write("QUIT");
		// C_GetServerState(GETS_State.All, null, null);
	}

	@ServerCommand(cmd = "JCPL")
	public void S_RecentlyCompletedJobInfo(int endTime, int jobId, String serverType, int serverId) {
		C_GetJobInfo("GQ", ListType.Number, null);
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
	public int limit;
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