package com.basho.proserv.datamigrator;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

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
		
		if (cmd.hasOption("l") && cmd.hasOption("d")) {
			System.out.println("Load (l) and Dump (d) are exclusive options");
			System.exit(1);
		}
		
		
		
		Configuration config = handleCommandLine(cmd);
		
		if (cmd.hasOption("l")) {
			runLoader(config);
		}

		if (cmd.hasOption("d")) {
			runDumper(config);
		}
		
	}
	
	public static Configuration handleCommandLine(CommandLine cmd) {
		Configuration config = new Configuration();
		
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
		
		if (cmd.hasOption("h")) {
			config.addHost(cmd.getOptionValue("h"));
		}
		
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
		
		if (cmd.hasOption("b")) {
			config.addBucketName(cmd.getOptionValue("b"));
		}
		if (cmd.hasOption("f")) {
			try {
				config.addBucketNames(Utilities.readFileLines(cmd.getOptionValue("f")));
			} catch (Exception e) {
				System.out.println("Could not read file containing buckets");
				System.exit(1);
			}
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
		if (cmd.hasOption("v")) {
			config.setVerboseStatus(true);
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

	public static void runLoader(Configuration config) {
		Connection connection = new Connection(config.getMaxRiakConnections());
		
		if (config.getHosts().size() == 1) {
			connection.connectPBClient(config.getHosts().toArray(new String[1])[0], config.getPort());
		} else {
			connection.connectPBCluster(config.getHosts(), config.getPort());
		}
		
		BucketLoader loader = new BucketLoader(connection, config.getFilePath(), 
				config.getVerboseStatus(), config.getRiakWorkerCount());
		
		long start = System.currentTimeMillis();
		int loadCount = 0;
		if (config.getOperation() == Configuration.Operation.BUCKETS) {
			loadCount = loader.LoadBuckets(config.getBucketNames());
		} else {
			loadCount = loader.LoadAllBuckets();
		}
		long stop = System.currentTimeMillis();
		
		connection.close();
		
		double totalTime = ((stop-start)/1000.0);
		Double recsPerSec = loadCount / totalTime;
		System.out.println("Loaded " + loadCount + " in " + totalTime + " seconds. " + recsPerSec + " objects/sec");
	}
	
	public static void runDumper(Configuration config) {
		Connection connection = new Connection(config.getMaxRiakConnections());
		if (config.getHosts().size() == 1) {
			connection.connectPBClient(config.getHosts().toArray(new String[1])[0], config.getPort());
		} else {
			connection.connectPBCluster(config.getHosts(), config.getPort());
		}
		
		BucketDumper dumper = new BucketDumper(connection, config.getFilePath(), 
				config.getVerboseStatus(), config.getRiakWorkerCount());
		
		long start = System.currentTimeMillis();
		int dumpCount = 0;
		if (config.getOperation() == Configuration.Operation.BUCKETS) {
			dumpCount = dumper.dumpBuckets(config.getBucketNames());
		} else {
			dumpCount = dumper.dumpAllBuckets();
		}
		long stop = System.currentTimeMillis();
		
		connection.close();
		
		double totalTime = ((stop-start)/1000.0);
		double recsPerSec = dumpCount / totalTime;
		System.out.println("Dumped " + dumpCount + " in " + totalTime + " seconds. " + recsPerSec + " objects/sec");
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
		options.addOption("r", true, "Set the path for data to be loaded to or dumped from. Required.");
		options.addOption("a", false, "Load or Dump all buckets");
		options.addOption("b", true, "Load or Dump a single bucket");
		options.addOption("f", true, "Load or Dump a file containing bucket names");
		options.addOption("h", true, "Specify Riak Host");
		options.addOption("c", true, "Specify a file containing Riak Cluster Host Names");
		options.addOption("p", true, "Specify Riak Port");
		options.addOption("v", false, "Output verbose status output to the command line");
		options.addOption("k", false, "Dump keys to file.  Cannot be used with l, d");
		options.addOption("j", true, "Resume based on previuosly written keys");
		options.addOption("riakworkercount", true, "Specify Riak Worker Count");
		options.addOption("maxriakconnections", true, "Specify the max number of connections maintained in the Riak Connection Pool");
		
		return options;
	}
	
}
