riak-data-migrator
========================
Tool for migrating data from one or more buckets in a Riak K/V store 
into another Riak K/V host or cluster by dumping all data from buckets 
to disk then allowing the user to load one or more of the dumped buckets
back into another Riak K/V host or cluster.

Building:
------------------------
Using Maven:  
```mvn package```  
Builds the project and creates a distributable tarball in the 
target/directory

Usage:
------------------------
Usage:  
```java -jar riak-data-migrator-0.1.jar [options]```

Options:  
-l Set to Load buckets. Cannot be used with d.  
-d Set to Dump buckets. Cannot be used with l.  
-r <path> Set the path for data to be loaded to or dumped from.
        The path must exist and is required.  
-a Load or Dump all buckets.  
-b <bucket name> Load or Dump a single bucket.  
-f <bucketNameFile.txt> Load or Dump a file containing line delimited
        bucket names.  
-h <hostName> Specify Riak host. Required if a cluster host name file is
        not specified.  
-c <clusterNameFile.txt> Specify a file containing line delimited Riak
        Cluster Host Names. Required if a host name is not specified.
        host name is not specified.  
-p <portNumber> Specify Riak Port. If not specified, defaults to 8087.  
-v Verbose output, shows number of ops/sec every second

Examples:
-------------------------
Dump all buckets from Riak:  
```java -jar riak-data-migrator-0.1.1.jar -d -r /var/riak_export -a -h 127.0.0.1 -p 8087```

Load all buckets previously dumped back into Riak:  
```java -jar riak-data-migrator-0.1.jar -l -r /var/riak-export -a -h 127.0.0.1 -p 8087```

Dump buckets listed in a line delimited file from a Riak cluster:  

<pre>
java -jar riak-data-migrator-0.1.jar -d -f /home/riakadmin/buckets_to_export.txt -r \  
/var/riak-export -c /home/riakadmin/riak_hosts.txt -p 8087
</pre>

Caveats:
------------------------
-This app depends on the key listing operation in the Riak client which
is slow on a good day.  
-The Riak memory backend bucket listing operating tends to timeout if
any significant amount of data exists.  In this case, you have to
explicitly specify the buckets you need want to dump using the -f
option to specify a line-delimited list of buckets in a file.  
