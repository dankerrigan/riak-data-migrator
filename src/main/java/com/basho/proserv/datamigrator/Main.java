package com.basho.proserv.datamigrator;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.basho.proserv.datamigrator.riak.Connection;

public class Main {

	public static void main(String[] args) {
		CommandLine cmd = null;
		try {
			cmd = parseCommandLine(createOptions(), args);
		} catch (ParseException e) {
			System.out.println("Error parsing command line. Reason: " + e.getMessage());
			System.exit(1);
		}
		
		
		// Handle exclusive options
		int cmdCount = 0;
		
		if (cmd.hasOption("l")) {
			++cmdCount;
		}
		if (cmd.hasOption("d")) {
			++cmdCount;
		}
		if (cmd.hasOption("k")) {
			++cmdCount;
		}
		
		
		if (cmdCount == 0) {
			System.out.println("You must specify l, d or k");
			System.exit(1);
		}
		if (cmdCount > 1) {
			System.out.println("Load (l), Dump (d), Keys and (k) are exclusive options.");
			System.exit(1);
		}
		
		if (cmd.hasOption('k') && cmd.hasOption('t')) {
			System.out.println("Keys (k) and Bucket Properties (t) are exclusive options.");
			System.exit(1);
		}
		
		
		if (cmd.hasOption('a') && cmd.hasOption('t')) {
			System.out.println("All Buckets (a) not compatible with Bucket Properties (t).");
			System.exit(1);
		}
		
		Configuration config = handleCommandLine(cmd);
		
		if (cmd.hasOption("l") || (cmd.hasOption("l") && cmd.hasOption("t"))) {
			runLoader(config);
		}

		if (cmd.hasOption("d") || cmd.hasOption("k") || (cmd.hasOption("d") && cmd.hasOption("t"))) {
			runDumper(config);
		}
		
	}
	
	public static Configuration handleCommandLine(CommandLine cmd) {
		Configuration config = new Configuration();
		
		// Data path
		if (cmd.hasOption("r")) {
			File dataPath = new File(cmd.getOptionValue("r"));
			if (!dataPath.exists()) {
				System.out.println("Data path " + dataPath.getAbsolutePath() + " does not exist.");
				System.exit(1);
			}
			config.setFilePath(dataPath);
		} else {
			System.out.println("Data path was not specified.");
			System.exit(1);
		}
		
		// Not available
//		if (cmd.hasOption("R")) {
//			config.setResume(true);
//		}
		
		// Host
		if (cmd.hasOption("h")) {
			config.addHost(cmd.getOptionValue("h"));
		}
		
		// Cluster hosts filename
		if (cmd.hasOption("c")) {
			try {
				config.addHosts(Utilities.readFileLines(cmd.getOptionValue("c")));
			} catch (Exception e) {
				System.out.println("Could not read file containting hosts." + e.getMessage());
				System.exit(1);
			}
		}
		if (config.getHosts().size() == 0) {
			System.out.println("No hosts specified");;
			System.exit(1);
		}
		
		// PB port
		if (cmd.hasOption("p")) {
			try {
				config.setPort(Integer.parseInt(cmd.getOptionValue("p")));
			} catch (NumberFormatException e) {
				System.out.println("Port (p) argument is not an integer.");
				System.exit(1);
			}
		} else {
			System.out.println("Port not specified, using the default: 8087");
		}
		
		// HTTP Port
		if (cmd.hasOption("H")) {
			try {
				config.setHttpPort(Integer.parseInt(cmd.getOptionValue("H")));
			} catch (NumberFormatException e) {
				System.out.println("HTTP port (H) argument is not an integer.");
				System.exit(1);
			}
		} else {
			System.out.println("HTTP port not specified, using the default: 8098");
		}
		
		// Single bucket specifier
		if (cmd.hasOption("b")) {
			config.addBucketName(cmd.getOptionValue("b"));
			config.setOperation(Configuration.Operation.BUCKETS);
		}
		// Bucket filename
		if (cmd.hasOption("f")) {
			try {
				config.addBucketNames(Utilities.readFileLines(cmd.getOptionValue("f")));
				config.setOperation(Configuration.Operation.BUCKETS);
			} catch (Exception e) {
				System.out.println("Could not read file containing buckets");
				System.exit(1);
			}
		}
		// Keys only
		if (cmd.hasOption("k")) { // if keys only....
			config.setOperation(Configuration.Operation.BUCKET_KEYS);
		}

		// Bucket properties transfer
		if (cmd.hasOption("t")) { // if transfer buckets, no compatible with k
			config.setOperation(Configuration.Operation.BUCKET_PROPERTIES);
		}
		
		if (config.getBucketNames().size() == 0 && !cmd.hasOption("a")) {
			System.out.println("No buckets specified to load");
			System.exit(1);
		}
		if (cmd.hasOption("a")) {
			if (config.getBucketNames().size() > 0) {
				System.out.println("Individual buckets specified as well as all buckets.  Dumping all buckets");
				config.setOperation(Configuration.Operation.ALL_BUCKETS);
			}
		}
		if (cmd.hasOption("k")) { // if keys only....
			config.setOperation(Configuration.Operation.ALL_KEYS);
		}
		
		//Verbose output
		if (cmd.hasOption("v")) {
			config.setVerboseStatus(true);
		}
		
		// not necessary...
//		if (cmd.hasOption("resetvclock")) {
//			config.setResetVClock(true);
//		}
		if (cmd.hasOption("riakworkercount")) {
			try {
				config.setRiakWorkerCount(Integer.parseInt(cmd.getOptionValue("riakworkercount")));
			} catch (Exception e) {
				System.out.println("Invalid value specified for riakworkercount");
				System.exit(1);
			}
		}
		
		if (cmd.hasOption("maxriakconnections")) {
			try {
				config.setMaxRiakConnectionsCount(Integer.parseInt(cmd.getOptionValue("maxriakconnections")));
			} catch (Exception e) {
				System.out.println("Invalid value specified for maxriakconnections");
				System.exit(1);
			}
		}
		return config;
	}

	public static void runLoader(Configuration config) {
		Connection connection = new Connection(config.getMaxRiakConnections());
		Connection httpConnection = new Connection();
		
		if (config.getHosts().size() == 1) {
			String host = config.getHosts().toArray(new String[1])[0];
			connection.connectPBClient(host, config.getPort());
			httpConnection.connectHTTPClient(host, config.getHttpPort());
		} else {
			connection.connectPBCluster(config.getHosts(), config.getPort());
			httpConnection.connectHTTPCluster(config.getHosts(), config.getHttpPort());
		}
		
		if (!connection.testConnection()) {
			System.out.println(String.format("Could not connect to Riak on PB port %d", config.getPort()));
			System.exit(-1);
		}
		if (!httpConnection.testConnection()) {
			System.out.println(String.format("Could not connect to Riak on HTTP port %d", config.getHttpPort()));
			System.exit(-1);
		}
		
		BucketLoader loader = new BucketLoader(connection, httpConnection, config.getFilePath(), 
				config.getVerboseStatus(), config.getRiakWorkerCount(), config.getResetVClock());
		
		long start = System.currentTimeMillis();
		long loadCount = 0;
		if (config.getOperation() == Configuration.Operation.BUCKETS) {
			loadCount = loader.LoadBuckets(config.getBucketNames());
		} else if (config.getOperation() == Configuration.Operation.BUCKET_PROPERTIES) {
			loadCount = loader.loadBucketSettings(config.getBucketNames());
		} else {
			loadCount = loader.LoadAllBuckets();
		}
		long stop = System.currentTimeMillis();
		
		connection.close();
		httpConnection.close();
		
		double totalTime = ((stop-start)/1000.0);
		Double recsPerSec = loadCount / totalTime;
		System.out.println("\nLoaded " + loadCount + " in " + totalTime + " seconds. " + recsPerSec + " objects/sec");
	}
	
	public static void runDumper(Configuration config) {
		Connection connection = new Connection(config.getMaxRiakConnections());
		Connection httpConnection = new Connection();
		
		if (config.getHosts().size() == 1) {
			String host = config.getHosts().toArray(new String[1])[0];
			connection.connectPBClient(host, config.getPort());
			httpConnection.connectHTTPClient(host, config.getHttpPort());
		} else {
			connection.connectPBCluster(config.getHosts(), config.getPort());
			httpConnection.connectHTTPCluster(config.getHosts(), config.getHttpPort());
		}
		
		if (!connection.testConnection()) {
			System.out.println(String.format("Could not connect to Riak on PB port %d", config.getPort()));
			System.exit(-1);
		}
		if (!httpConnection.testConnection()) {
			System.out.println(String.format("Could not connect to Riak on HTTP port %d", config.getHttpPort()));
			System.exit(-1);
		}
		
		BucketDumper dumper = new BucketDumper(connection, httpConnection, config.getFilePath(), 
				config.getVerboseStatus(), config.getRiakWorkerCount());
		
		
		boolean keysOnly = (config.getOperation() == Configuration.Operation.ALL_KEYS ||
				config.getOperation() == Configuration.Operation.BUCKET_KEYS);
		long start = System.currentTimeMillis();
		long dumpCount = 0;
		if (config.getOperation() == Configuration.Operation.BUCKETS || 
				config.getOperation() == Configuration.Operation.BUCKET_KEYS) {
			dumpCount = dumper.dumpBuckets(config.getBucketNames(), config.getResume(), keysOnly);
		} else if (config.getOperation() == Configuration.Operation.BUCKET_PROPERTIES) {
			dumpCount = dumper.dumpBucketSettings(config.getBucketNames());
		} else {
			dumpCount = dumper.dumpAllBuckets(config.getResume(), keysOnly);
		}
		long stop = System.currentTimeMillis();
		
		connection.close();
		httpConnection.close();
		
		double totalTime = ((stop-start)/1000.0);
		double recsPerSec = dumpCount / totalTime;
		System.out.println("\nDumped " + dumpCount + " in " + totalTime + " seconds. " + recsPerSec + " objects/sec");
	}
	
	public static void printHelp(String arg) {
		Options options = createOptions();
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(arg, options);
	}
	
	private static CommandLine parseCommandLine(Options options, String[] args) throws ParseException {
		CommandLineParser parser = new GnuParser();
		CommandLine cmd = parser.parse(options, args);
		return cmd;
	}

	private static Options createOptions() {
		Options options = new Options();
		
		options.addOption("l", false, "Set to Load buckets. Cannot be used with d, k");
		options.addOption("d", false, "Set to Dump buckets. Cannot be used with l, k");
		options.addOption("R", false, "Configure tool to resume previous operation");
		options.addOption("r", true, "Set the path for data to be loaded to or dumped from. Required.");
		options.addOption("a", false, "Load or Dump all buckets");
		options.addOption("b", true, "Load or Dump a single bucket");
		options.addOption("f", true, "Load or Dump a file containing bucket names");
		options.addOption("h", true, "Specify Riak Host");
		options.addOption("c", true, "Specify a file containing Riak Cluster Host Names");
		options.addOption("p", true, "Specify Riak PB Port");
		options.addOption("H", true, "Specify Riak HTTP Port");
		options.addOption("v", false, "Output verbose status output to the command line");
		options.addOption("k", false, "Dump keys to file.  Cannot be used with l, d");
		options.addOption("t", false, "Download bucket properties");
//		options.addOption("j", true, "Resume based on previuosly written keys");
		options.addOption("resetvclock", false, "Resets object's VClock prior to being loaded in Riak");
		options.addOption("riakworkercount", true, "Specify Riak Worker Count");
		options.addOption("maxriakconnections", true, "Specify the max number of connections maintained in the Riak Connection Pool");
		
		return options;
	}
	
}
