package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.proserv.datamigrator.io.Key;

public interface IClientDeleter {
	public Key deleteKey(Key key) throws IOException;
}
