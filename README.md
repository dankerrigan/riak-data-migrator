riak-data-migrator
========================
Tool for migrating data from one or more buckets in a Riak K/V store 
into another Riak K/V host or cluster by dumping all data from buckets 
to disk then allowing the user to load one or more of the dumped buckets
back into another Riak K/V host or cluster.

Downloading:
------------------------
You can download the ready to run jar file at:
http://ps-tools.data.riakcs.net:8080/riak-data-migrator-0.1.3-bin.tar.gz

After downloading, unzip/untar it, and it's ready to run from its directory.
```bash
tar -xvzf riak-data-migrator-0.1.3-bin.tar.gz
cd riak-data-migrator-0.1.3
java -jar riak-data-migrator-0.1.3.jar [options]
```

Building from source:
------------------------
1. Make sure [Apache Maven](http://maven.apache.org/) is installed
2. Riak Data Migrator depends on the [Riak Java Client](https://github.com/basho/riak-java-client),
    and it needs to be built first
```bash
git clone https://github.com/basho/riak-java-client.git
cd riak-java-client.git
mvn install
cd ..
```

3. Build the Riak Data Migrator itself, using Maven. First, fork this project, and ```git clone``` your fork.
```bash
cd riak-data-migrator.git
mvn clean
mvn package
```

    The compiled .jar file is located in the ```target/``` directory.
    The usable binary file is ```riak-data-migrator-0.1.3-bin.tar.gz```  (the other .tar.gz is the source archive).

Usage:
------------------------
Usage:  
```java -jar riak-data-migrator-0.1.3.jar [options]```

Options:
```
-l Set to Load buckets. Cannot be used with d or k.  
-d Set to Dump buckets. Cannot be used with l or k.  
-k Set to Dump bucket keys. Cannot be used with d or l.
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
-p <pbcPortNumber> Specify Riak Protocol Buffers Port. If not specified, defaults to 8087.
-H <httpPortNumber> Specify Riak HTTP Port. If not specified, defaults to 8098.
-v Verbose output, shows number of ops/sec every second
--riakworkercount Specify the number of workers used to read from/write 
    to Riak.
--maxriakconnections Specify the number of connections to maintain
    in the Riak connection pool. 
```

Examples:
-------------------------
Dump all buckets from Riak:  
```java -jar riak-data-migrator-0.1.3.jar -d -r /var/riak_export -a -h 127.0.0.1 -p 8087 -H 8098```

Load all buckets previously dumped back into Riak:  
```java -jar riak-data-migrator-0.1.3.jar -l -r /var/riak-export -a -h 127.0.0.1 -p 8087 -H 8098```

Dump buckets listed in a line delimited file from a Riak cluster:  

<pre>
java -jar riak-data-migrator-0.1.3.jar -d -f /home/riakadmin/buckets_to_export.txt -r \  
/var/riak-export -c /home/riakadmin/riak_hosts.txt -p 8087 -H 8098
</pre>

Caveats:
------------------------
-This app depends on the key listing operation in the Riak client which
is slow on a good day.  
-The Riak memory backend bucket listing operating tends to timeout if
any significant amount of data exists.  In this case, you have to
explicitly specify the buckets you need want to dump using the -f
option to specify a line-delimited list of buckets in a file.  
