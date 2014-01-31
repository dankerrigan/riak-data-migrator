package com.basho.proserv.datamigrator.io;

import com.basho.proserv.datamigrator.Utilities;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.pbc.RiakObject;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: dankerrigan
 * Date: 1/28/14
 * Time: 11:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class BucketKeyJournal extends AbstractKeyJournal {
    private final String bucketName;

    public BucketKeyJournal(File path, Mode mode, String bucketName) {
        super(path, mode);

        this.bucketName = bucketName;
    }

    public void write(Key key) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void write(String bucket, String key) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void write(RiakObject riakObject) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void write(IRiakObject riakObject) throws IOException {
        throw new UnsupportedOperationException();
    }

    public Key read() throws IOException {
        if (this.mode == Mode.WRITE) {
            throw new IllegalArgumentException("KeyJournal is in WRITE mode for read operation");
        }
        String line = this.reader.readLine();
        if (line == null) {
            return null;
        }

        return new Key(this.bucketName, Utilities.urlDecode(line));
    }
}
