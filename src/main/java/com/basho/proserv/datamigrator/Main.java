package com.basho.proserv.datamigrator;

import java.io.File;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		if (cmd.hasOption("delete")) {
			++cmdCount;
		}
		if (cmd.hasOption("copy")) {
			++cmdCount;
		}
		
		
		if (cmdCount == 0) {
			System.out.println("You must specify l, d, k, copy, or delete");
			System.exit(1);
		}
		if (cmdCount > 1) {
			System.out.println("Load (l), Dump (d), Keys (k), Copy (copy), and Delete (delete) are exclusive options.");
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
		
		if (cmd.hasOption("delete") && cmd.hasOption('t')) {
			System.out.println("Delete (delete) not compatible with Bucket Properties (t).");
			System.exit(1);
		}
		
		if (cmd.hasOption("delete") && cmd.hasOption("a")) {
			System.out.println("Delete not compatible with All (a) option. Specify buckets indidually instead.");
		}
		
		if (cmd.hasOption("copy") && (cmd.hasOption('t') || cmd.hasOption('k'))) {
			System.out.println("Copy not compatible with t or k options.");
		}
		
		Configuration config = handleCommandLine(cmd);
		
		if (cmd.hasOption("delete")) {
			runDelete(config);
		}
		
		if (cmd.hasOption("copy")) {
			runCopy(config);
		}
		
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
		if (!cmd.hasOption("copy")) {
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
		
		if (cmd.hasOption("copyhost")) {
			config.addDestinationHost(cmd.getOptionValue("copyhost"));
		}
		
		if (cmd.hasOption("copyhostsfile")) {
			try {
				config.addDestinationHosts(Utilities.readFileLines(cmd.getOptionValue("copyhostsfiles")));
			} catch (Exception e) {
				System.out.println("Could not read file containting hosts." + e.getMessage());
				System.exit(1);
			}
		}
		if (cmd.hasOption("copy") && config.getDestinationHosts().size() == 0) {
			System.out.println("No destination hosts specified");;
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
		
		// Destination PB port
		if (cmd.hasOption("copypbport")) {
			try {
				config.setPort(Integer.parseInt(cmd.getOptionValue("copypbport")));
			} catch (NumberFormatException e) {
				System.out.println("Destination PB Port (copypbport) argument is not an integer.");
				System.exit(1);
			}
		} else {
			System.out.println("Destination PB Port not specified, using the default: 8087");
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

		// Dump from a list of buckets/keys
		if (cmd.hasOption("K")) {
			try {
				String fileName = cmd.getOptionValue("K");
				config.addKeyNames(Utilities.readFileLines(fileName));
				config.setOperation(Configuration.Operation.KEYS);
			} catch (Exception e) {
				System.out.println("Could not read file containing list of bucket,keys");
				System.exit(1);
			}
		}

		// Keys only
		if (cmd.hasOption("k")) { // if keys only....
			config.setOperation(Configuration.Operation.BUCKET_KEYS);
		}

		// Bucket properties transfer
		if (cmd.hasOption("t")) { // if transfer buckets, not compatible with k
			config.setOperation(Configuration.Operation.BUCKET_PROPERTIES);
		}
		
		
		if (config.getBucketNames().size() == 0 && !cmd.hasOption("a") && !cmd.hasOption("K")) {
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
			if (config.getBucketNames().size() > 0) {
				config.setOperation(Configuration.Operation.BUCKET_KEYS);
			} else {
				config.setOperation(Configuration.Operation.ALL_KEYS);
			}
		}
		if (cmd.hasOption("delete")) {
			if (config.getBucketNames().size() > 0) {
				config.setOperation(Configuration.Operation.DELETE_BUCKETS);
			}
		}
		
		// Bucket Copy
		if (cmd.hasOption("copy") && config.getBucketNames().size() > 0) {
			config.setOperation(Configuration.Operation.COPY_BUCKETS);
		}
		if (cmd.hasOption("copy") && cmd.hasOption("a")) {
			config.setOperation(Configuration.Operation.COPY_ALL);
		}
		
		if (cmd.hasOption("destinationbucket")) {
			if (config.getBucketNames().size() != 1) {
				System.out.println("Destination bucket option only valid when specifying a single bucket.");
				System.exit(1);
			}
			config.setDestinationBucket(cmd.getOptionValue("destinationbucket"));
		}
		
		if (cmd.hasOption("q")) {
			int queueSize = Integer.parseInt(cmd.getOptionValue("q"));
			config.setQueueSize(queueSize);
		}
		
		//Verbose output - now default
		if (cmd.hasOption("v")) {
			config.setVerboseStatus(true);
		}
		// Turn off verbose output
		if (cmd.hasOption("s")) {
			config.setVerboseStatus(false);
		}
		
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

	public static void runDelete(Configuration config) {
		Connection connection = new Connection(config.getMaxRiakConnections());
		
		if (config.getHosts().size() == 1) {
			String host = config.getHosts().toArray(new String[1])[0];
			connection.connectPBClient(host, config.getPort());
		} else {
			connection.connectPBCluster(config.getHosts(), config.getPort());
		}
		
		if (!connection.testConnection()) {
			System.out.println(String.format("Could not connect to Riak on PB port %d", config.getPort()));
			System.exit(-1);
		}
		
		BucketDelete deleter = new BucketDelete(connection, config.getVerboseStatus());

		@SuppressWarnings("unused")
		long deleteCount = 0;
		if (config.getOperation() == Configuration.Operation.DELETE_BUCKETS) {
			deleteCount = deleter.deleteBuckets(config.getBucketNames());
		}
		
		connection.close();

		printSummary(deleter.summary, "Load Summary:");
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
		
		BucketLoader loader = new BucketLoader(connection, httpConnection, config);
		
		@SuppressWarnings("unused")
		long loadCount = 0;
		if (config.getOperation() == Configuration.Operation.BUCKETS) {
			loadCount = loader.LoadBuckets(config.getBucketNames());
		} else if (config.getOperation() == Configuration.Operation.BUCKET_PROPERTIES) {
			loadCount = loader.loadBucketSettings(config.getBucketNames());
		} else {
			loadCount = loader.LoadAllBuckets();
		}
		
		connection.close();
		httpConnection.close();
		
		printSummary(loader.summary, "Load Summary:");
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
		
		BucketDumper dumper = new BucketDumper(connection, httpConnection, config);
		
		
		boolean keysOnly = (config.getOperation() == Configuration.Operation.ALL_KEYS ||
				config.getOperation() == Configuration.Operation.BUCKET_KEYS);

		@SuppressWarnings("unused")
		long dumpCount = 0;
		if (config.getOperation() == Configuration.Operation.BUCKETS || 
				config.getOperation() == Configuration.Operation.BUCKET_KEYS) {
			dumpCount = dumper.dumpBuckets(config.getBucketNames(), config.getResume(), keysOnly);
		} else if (config.getOperation() == Configuration.Operation.BUCKET_PROPERTIES) {
			dumpCount = dumper.dumpBucketSettings(config.getBucketNames());
		} else if (config.getOperation() == Configuration.Operation.KEYS) {
			dumpCount = dumper.dumpKeys(config.getKeyNames());
		} else {
			dumpCount = dumper.dumpAllBuckets(config.getResume(), keysOnly);
		}
		
		connection.close();
		httpConnection.close();

		printSummary(dumper.summary, "Dump Summary:");
	}
	
	private static void runCopy(Configuration config) {
		Connection fromConnection = new Connection(config.getMaxRiakConnections());
		Connection toConnection = new Connection(config.getMaxRiakConnections());
		
		if (config.getHosts().size() == 1) {
			String host = config.getHosts().toArray(new String[1])[0];
			fromConnection.connectPBClient(host, config.getPort());
		} else {
			fromConnection.connectPBCluster(config.getHosts(), config.getPort());
		}
		
		if (config.getDestinationHosts().size() == 1) {
			String host = config.getDestinationHosts().toArray(new String[1])[0];
			toConnection.connectPBClient(host, config.getDestinationPort());
		} else {
			toConnection.connectPBCluster(config.getDestinationHosts(), config.getDestinationPort());
		}
		
		if (!fromConnection.testConnection()) {
			System.out.println(String.format("Could not connect to Riak on PB port %d", config.getPort()));
			System.exit(-1);
		}
		if (!toConnection.testConnection()) {
			System.out.println(String.format("Could not connect to destination Riak on PB port %d", config.getDestinationPort()));
			System.exit(-1);
		}
		
		BucketTransfer mover = new BucketTransfer(fromConnection, toConnection, config);
		
		@SuppressWarnings("unused")
		long transferCount = 0;
		if (config.getOperation() == Configuration.Operation.COPY_BUCKETS) {
			transferCount = mover.transferBuckets(config.getBucketNames(), false);
		} else {
			transferCount = mover.transferAllBuckets(false);
		}
		
		fromConnection.close();
		toConnection.close();
		
		printSummary(mover.summary, "Copy Summary:");
		
	}
	
	public static void printHelp(String arg) {
		Options options = createOptions();
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(arg, options);
	}
	
	private static void printSummary(Summary summary, String title) {
		Logger log = LoggerFactory.getLogger("status");
		
		Map<String, Long[]> bucketStats = summary.getStatistics();
		System.out.println();
		System.out.println(title);
		log.info(title);
		String header = String.format("%15s%12s%12s%12s%12s%10s","Bucket","Objects","Seconds","Objs/Sec","Size/KB","Val. Err.");
		System.out.println(header);
		log.info(header);
		long totalRecords = 0;
		long totalTime = 0;
		long totalSize = 0;
		long totalValueErrors = 0;
		for (String bucketName : summary.bucketNames()) {
			Long[] count_time = bucketStats.get(bucketName);
			String line = null;
			if (count_time[0] < 0) {
				String errorString = "ERROR";
				if (count_time[0] == -2) {
					errorString = "KEY LIST ERROR";
				} else if (count_time[0] == -3) {
					errorString = "BUCKET DELETE ERROR";
				}
				line = String.format("%15s%12s%12s%12s%12s%10s", 
						bucketName, 
						errorString,
						errorString,
						errorString,
						errorString,
						errorString);
			} else {
				totalRecords += count_time[0];
				totalTime += count_time[1];
				totalSize += count_time[2];
				totalValueErrors += count_time[3];
				line = String.format("%15s%12d%12.1f%12.1f%12d%10d",
						bucketName,
						count_time[0],
						count_time[1]/1000.0,
						count_time[0]/(count_time[1]/1000.0),
						count_time[2]/1024,
						count_time[3]);
			}
			System.out.println(line);
			log.info(line);
		}
		String line = String.format("%15s%12d%12.1f%12.1f%12d%10d",
				String.format("Total: %d", summary.bucketNames().size()),
				totalRecords,
				totalTime/1000.0,
				totalRecords/(totalTime/1000.0),
				totalSize/1024,
				totalValueErrors
				);
		System.out.println(line);
		log.info(line);
	}
	
	private static CommandLine parseCommandLine(Options options, String[] args) throws ParseException {
		CommandLineParser parser = new GnuParser();
		CommandLine cmd = parser.parse(options, args);
		return cmd;
	}

    //TODO: add option that reads a partial list of keys from file to backup (in same format as -k)

	private static Options createOptions() {
		Options options = new Options();
		
		options.addOption("l", false, "Set to Load buckets. Cannot be used with d, k");
		options.addOption("d", false, "Set to Dump buckets. Cannot be used with l, k");
		options.addOption("R", false, "Configure tool to resume previous operation");
		options.addOption("r", true, "Set the path for data to be loaded to or dumped from. Required.");
		options.addOption("a", false, "Load or Dump all buckets");
		options.addOption("b", true, "Load or Dump a single bucket");
		options.addOption("f", true, "Load or Dump a file containing bucket names");
		options.addOption("K", true, "Load or Dump a file containing bucket names and keys");
		options.addOption("h", true, "Specify Riak Host");
		options.addOption("c", true, "Specify a file containing Riak Cluster Host Names");
		options.addOption("p", true, "Specify Riak PB Port");
		options.addOption("H", true, "Specify Riak HTTP Port");
		options.addOption("v", false, "Output verbose status output to the command line (default)");
		options.addOption("s", true, "Suppress normal verbose output");
		options.addOption("k", false, "Dump keys to file.  Cannot be used with l, d");
		options.addOption("t", false, "Download bucket properties");
		options.addOption("q", true, "Set the queue Size");
		options.addOption("copy", false, "Copy bucket/buckets to a different Riak Cluster. Cannot be used with d, k, l, or delete");
		options.addOption("copyhost", true, "Copy destination host");
		options.addOption("copyhostsfile", true, "Specify file containing destination cluster hosts");
		options.addOption("copypbport", true, "Copy destination protocol buffers port");
		options.addOption("destinationbucket", true, "Destination Bucket name for single bucket copy");
//		options.addOption("j", true, "Resume based on previously written keys");
		options.addOption("resetvclock", false, "Resets object's VClock prior to being loaded in Riak");
		options.addOption("riakworkercount", true, "Specify Riak Worker Count");
		options.addOption("maxriakconnections", true, "Specify the max number of connections maintained in the Riak Connection Pool");
		options.addOption("delete", false, "Delete specified buckets");
		return options;
	}
	
}
