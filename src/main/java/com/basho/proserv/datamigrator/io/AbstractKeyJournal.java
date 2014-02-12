package com.basho.proserv.datamigrator.io;

import com.basho.proserv.datamigrator.Utilities;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.pbc.RiakObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;


public abstract class AbstractKeyJournal implements Iterable<Key>, IKeyJournal {
    private final Logger log = LoggerFactory.getLogger(AbstractKeyJournal.class);

    public enum Mode { READ, WRITE }

    protected final File path;
    protected final Mode mode;
    protected final BufferedWriter writer;
    protected final BufferedReader reader;
    private long keyCount;
    private boolean closed = false;

    public AbstractKeyJournal(File path, Mode mode) {
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        this.path = path;
        try {
            if (mode == Mode.WRITE) {
                this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)));
                this.keyCount = 0;
                this.reader = null;
            } else {
                this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
                this.keyCount = -1;
                this.writer = null;
            }
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Could not open " + path.getAbsolutePath());
        }

        this.mode = mode;
    }

    public void populate(String bucketName, Iterable<String> keys) throws IOException {
        for (String keyString : keys) {
            this.write(bucketName, keyString);
        }
    }

    public long getKeyCount() {
        if (this.mode == Mode.READ && keyCount == -1) {
            try {
                this.keyCount = this.countKeys(this.path);
            } catch (IOException ex) {
                log.error("Could not read key file for counting");
            }
        }
        return this.keyCount;
    }

    @Override
    public void write(Key key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        this.write(key.bucket(), key.key());
    }

    @Override
    public void write(String bucket, String key) throws IOException {
        if (mode == Mode.READ) {
            throw new IllegalArgumentException ("KeyJournal is in READ mode for write operation");
        }
        if (bucket == null || key == null) {
            throw new IllegalArgumentException("bucket and key must not be null");
        }
        this.writer.write((Utilities.urlEncode(bucket) + "," + Utilities.urlEncode(key) + "\n"));
        this.keyCount++;
    }

    @Override
    public void write(RiakObject riakObject) throws IOException {
        this.write(riakObject.getBucket(), riakObject.getKey());
    }

    @Override
    public void write(IRiakObject riakObject) throws IOException {
        this.write(riakObject.getBucket(), riakObject.getKey());
    }

    @Override
    public Key read() throws IOException {
        if (mode == Mode.WRITE) {
            throw new IllegalArgumentException("KeyJournal is in WRITE mode for read operation");
        }
        String line = this.reader.readLine();
        if (line == null) {
            return null;
        }
        String[] values = new String[2];
        int comma = line.indexOf(',');
        if (comma != -1) {
            values[0] = line.substring(0, comma);
            values[1] = line.substring(comma + 1, line.length());
            return new Key(Utilities.urlDecode(values[0]), Utilities.urlDecode(values[1]));
        }
        return null;
    }

    public void close() {
        try {
            if (this.writer != null) {
                this.writer.flush();
                this.writer.close();
            }
            if (this.reader != null) {
                this.reader.close();
            }
        } catch (IOException e) {
            // no-op, swallow
        }
        this.closed = true;
    }

    public boolean isClosed() {
        return this.closed;
    }

    public static File createKeyPathFromPath(File file, boolean load) {
        String path = file.getAbsolutePath();
        int ind = path.lastIndexOf('.');
        if (ind == -1) {
            ind = path.length()-1;
        }
        path = path.substring(0, ind);
        if (load) {
            path = path + ".loadedkeys";
        } else {
            path = path + ".keys";
        }
        return new File(path);
    }

    protected long countKeys(File path) throws IOException, FileNotFoundException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));

        long count = 0;
        while (reader.readLine() != null) {
            ++count;
        }

        reader.close();

        return count;
    }

    @Override
    public Iterator<Key> iterator() {
        return new KeyIterator(this);
    }

    private class KeyIterator implements Iterator<Key> {
        private final IKeyJournal keyJournal;

        private Key nextKey;

        public KeyIterator(IKeyJournal keyJournal) {
            this.keyJournal = keyJournal;
            try {
                this.nextKey = keyJournal.read();
            } catch (IOException e) {
                this.nextKey = null;
            }
        }

        @Override
        public boolean hasNext() {
            return this.nextKey != null;
        }

        @Override
        public Key next() {
            Key currentKey = this.nextKey;
            try {
                this.nextKey = this.keyJournal.read();
            } catch (IOException e) {
                this.nextKey = null;
            }
            if (currentKey == null && this.nextKey == null) {
                currentKey = Key.createErrorKey();
            }
            return currentKey;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}

