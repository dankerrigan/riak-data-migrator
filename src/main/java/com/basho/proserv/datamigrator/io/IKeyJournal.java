package com.basho.proserv.datamigrator.io;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.pbc.RiakObject;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: dankerrigan
 * Date: 1/28/14
 * Time: 10:55 AM
 * To change this template use File | Settings | File Templates.
 */
public interface IKeyJournal extends Iterable<Key> {
    void populate(String bucketName, Iterable<String> keys) throws IOException;

    void write(Key key) throws IOException;

    void write(String bucket, String key) throws IOException;

    void write(RiakObject riakObject) throws IOException;

    void write(IRiakObject riakObject) throws IOException;

    Key read() throws IOException;

    void close();

    long getKeyCount();

    Iterator<Key> iterator();
}
