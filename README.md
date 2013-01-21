riak-data-migrator

Tool for migrating data from one or more buckets in a Riak K/V store 
into another Riak K/V host or cluster by dumping all data from buckets 
to disk then allowing the user to load one or more of the dumped buckets
back into another Riak K/V host or cluster.

Building:

Using Maven:
'mvn package' builds the project and creates a distributable tarball
in the target/ directory

Usage:
See the README file in the root of the proejct.  This file is also
packaged in the distribable tarball.

Caveats:
-This app depends on the key listing operation in the Riak client which
is slow on a good day.
-The Riak memory backend bucket listing operating tends to timeout if
any signifigant amount of data exists.  In this case, you have to
use explicitly specify the buckets you need want to dump using the -f
option to specify a line-delimited list of buckets in a file.
