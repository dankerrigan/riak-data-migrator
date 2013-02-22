riak-data-migrator
========================
Tool for migrating data from one or more buckets in a [Riak](http://basho.com/riak/) K/V store 
into another Riak cluster by exporting all data from buckets 
to disk, and then allowing the user to load one or more of the dumped buckets
back into another Riak K/V host or cluster.

Intended Usage
------------------------
This data migrator tool may be helpful in the following scenarios:

1. Migrating an entire non-live Riak cluster (no new writes are coming in) to a cluster with a different ring size.
2. Exporting the contents of a single bucket to disk (again, on a non-live cluster where no new writes are coming in)
3. Exporting a *list of keys* only, from a particular bucket to a text file (same conditions as above)
4. Taking an approximate snapshot of live data for a QA cluster (approximate because, if new objects are written
    to the cluster after the export operation starts, they are not guaranteed to be exported in that session)

The app works by performing a streaming [List Keys](http://docs.basho.com/riak/latest/references/apis/http/HTTP-List-Keys/)
operation on one or more Riak buckets, and then issuing GETs for the resulting keys (parallelized as much as possible) and
storing the Riak objects (in [Protocol Buffer](http://docs.basho.com/riak/latest/references/apis/protocol-buffers/) format) on disk.
*The key listing alone involves multiple iterations through the entire Riak keyspace, and is not intended for frequent
usage on a live cluster.* 
On the import side, the app reads the exported Riak objects from files on disk, and issues PUTs to the target cluster.

**Do NOT** use for regular data backup on a live cluster. Instead, see the [Backing Up Riak](http://docs.basho.com/riak/latest/cookbooks/Backups/) 
documentation for recommended best practices. Again, the reason for this admonition is: if new data is written to a live cluster
after the export operation is started, that new data is not guaranteed to be included in the export.

Workflow
------------------------
To transfer data from one Riak cluster to another:

1. *Before using the migrator tool*, make sure that the ```app.config``` files are the same in both clusters. 
    Meaning, settings such as default quorum values, multi-backend and MDC replication settings, should be the same
    before starting export/import operations.
2. (Optional) If applicable, transfer custom bucket [properties](http://docs.basho.com/riak/latest/tutorials/fast-track/Basic-Riak-API-Operations/#Bucket-Properties-and-Operations)
    using the ```-d -t``` options to export from one cluster and ```-l -t``` to import into the target cluster.
    Note: Do this *only* if you set custom properties such as non-default [quorum values](http://docs.basho.com/riak/latest/tutorials/fast-track/Tunable-CAP-Controls-in-Riak/),
    pre- or [post-commit hooks](http://docs.basho.com/riak/latest/references/appendices/concepts/Commit-Hooks/), or 
    set up [Search indexing](http://docs.basho.com/riak/latest/cookbooks/Riak-Search---Indexing-and-Querying-Riak-KV-Data/#Setting-up-Indexing)
    on a bucket.
3. Export the contents of a bucket (Riak objects) using the ```-d``` option, to files on disk (the objects will be stored in 
    the binary [ProtoBuf](http://docs.basho.com/riak/latest/references/apis/protocol-buffers/) format)
4. Load the Riak objects from the exported files into the target cluster using the ```-l``` option.

Downloading:
------------------------
You can download the ready to run jar file at:
http://ps-tools.data.riakcs.net:8080/riak-data-migrator-0.1.4-bin.tar.gz

After downloading, unzip/untar it, and it's ready to run from its directory.
```bash
tar -xvzf riak-data-migrator-0.1.4-bin.tar.gz
cd riak-data-migrator-0.1.4
java -jar riak-data-migrator-0.1.4.jar [options]
```

Building from source:
------------------------
1. Make sure [Apache Maven](http://maven.apache.org/) is installed

2. Build the Riak Data Migrator itself, using Maven. First, fork this project, and ```git clone``` your fork.
```bash
cd riak-data-migrator
mvn clean
mvn package
```

    The compiled .jar file is located in the ```target/``` directory.
    The usable binary file is ```riak-data-migrator-0.1.4-bin.tar.gz```

Usage:
------------------------
Usage:  
```java -jar riak-data-migrator-0.1.4.jar [options]```

Options:
```
Data Transfer (required, one of: d, l or k)
-d Export (Dump) the contents bucket(s) (keys and objects), in ProtoBuf format, to files
-l Import (Load) the contents of bucket(s) exported by the data migrator, from files.
-k Export a list of Keys only (not object values) from bucket(s), to a text file. (Not to be used with -t)

Settings Transfer (optional, used with to -d or -l)
-t Transfer custom bucket properties only (no data), to and from files on disk. 
   You must specify one or more buckets (not to be used with -a, the All Buckets option).
   You must also specify -d to export or -l to import, with this option.

Path (required)
-r <path> Set the path for data to be loaded to or dumped from (path must be valid)

Bucket Options (required for -d, -k or -t)
-a Export all buckets.
-b <bucket name> Export a single bucket.  
-f <bucketNameFile.txt> Export multiple buckets listed in a file (containing line-delimited bucket names)

Cluster Addresses and Ports (required)
-h <hostName> Specify Riak hostname. Required if a cluster host name file is not specified.  
-c <clusterNameFile.txt> Specify a file containing line delimited Riak Cluster Host
   Names. Required if a host name is not specified. Host name is not specified.  
-p <pbcPortNumber> Specify Riak Protocol Buffers Port. If not specified, defaults to 8087.  
-H <httpPortNumber> Specify Riak HTTP Port. If not specified, defaults to 8098.  

Concurrency and Misc Settings
--riakworkercount Specify the number of worker threads used to read from/write 
  to Riak (defaults to: 2 * #processor_cores)
--maxriakconnections Specify the max number of connections to maintain
  in the Riak connection pool (defaults to: 2 * workercount, see above)
-v Verbose output, shows number of ops/sec every second.
```

Examples:
-------------------------
Dump (the contents of) all buckets from Riak:  
```java -jar riak-data-migrator-0.1.4.jar -d -r /var/riak_export -a -h 127.0.0.1 -p 8087 -H 8098```

Load all buckets previously dumped back into Riak:  
```java -jar riak-data-migrator-0.1.4.jar -l -r /var/riak-export -a -h 127.0.0.1 -p 8087 -H 8098```

Dump (the contents of) buckets listed in a line delimited file from a Riak cluster:  
<pre>
java -jar riak-data-migrator-0.1.3.jar -d -f /home/riakadmin/buckets_to_export.txt -r \  
/var/riak-export -c /home/riakadmin/riak_hosts.txt -p 8087 -H 8098
</pre>

Export only the bucket settings from a bucket named "Flights":  
```java -jar riak-data-migrator-0.1.4.jar -d -t -r /var/riak-export -b Flights -h 127.0.0.1 -p 8087 -H 8098```

Load bucket settings for a bucket named "Flights":  
```java -jar riak-data-migrator-0.1.4.jar -l -t -r /var/riak-export -b Flights -h 127.0.0.1 -p 8087 -H 8098```

Caveats:
------------------------
-This app depends on the key listing operation in the Riak client which
is slow on a good day.  
-The Riak memory backend bucket listing operating tends to timeout if
any significant amount of data exists.  In this case, you have to
explicitly specify the buckets you need want to dump using the ```-f```
option to specify a line-delimited list of buckets in a file.  
