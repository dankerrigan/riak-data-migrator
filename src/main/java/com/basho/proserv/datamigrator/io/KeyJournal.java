package com.basho.proserv.datamigrator.io;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class KeyJournal extends AbstractKeyJournal {

	private long writeCount;
	private boolean closed = false;
	
	public KeyJournal(File path, Mode mode) {
        super(path, mode);

	}

	
}
