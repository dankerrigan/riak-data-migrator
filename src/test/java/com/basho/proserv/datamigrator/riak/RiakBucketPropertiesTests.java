package com.basho.proserv.datamigrator.riak;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.cap.Quorum;
import com.basho.riak.client.query.functions.NamedErlangFunction;

public class RiakBucketPropertiesTests {

	@Test
	public void testWriteReadBucketProperties() throws Exception {
		TemporaryFolder tempFolder = new TemporaryFolder();
		File propsFile = tempFolder.newFile();
		
		BucketPropertiesBuilder builder = new BucketPropertiesBuilder();
		builder.addPrecommitHook(new NamedErlangFunction("named", "function"));
		builder.addPostcommitHook(new NamedErlangFunction("bob","harry"));
		builder.allowSiblings(true);
		builder.backend("dandb");
		builder.basicQuorum(true);
		builder.bigVClock(1234567890);
		builder.chashKeyFunction(new NamedErlangFunction("sally", "quenton"));
//		builder.dw(1);
//		builder.dw(Quora.fromString("one"));
		builder.dw(new Quorum(1));
		builder.lastWriteWins(true);
		builder.linkWalkFunction(new NamedErlangFunction("whos", "on first"));
		builder.notFoundOK(true);
		builder.nVal(2);
		builder.oldVClock(1234567889);
//		builder.pr(1);
//		builder.pr(Quora.fromString("one"));
		builder.pr(new Quorum(1));
//		builder.pw(1);
//		builder.pw(Quora.fromString("one"));
		builder.pw(new Quorum(1));
//		builder.r(1);
//		builder.r(Quora.fromString("one"));
		builder.r(new Quorum(1));
//		builder.rw(1);
//		builder.rw(Quora.fromString("one"));
		builder.rw(new Quorum(1));
		builder.search(true);
		builder.smallVClock(123456789);
//		builder.w(1);
//		builder.w(Quora.fromString("one"));
		builder.w(new Quorum(1));
		builder.youngVClock(1234567);
		
		BucketProperties props =  builder.build();
		
		RiakBucketProperties bucketProps = new RiakBucketProperties(new Connection());
		
		boolean success = bucketProps.writeBucketProperties(props, propsFile);
		
		assertTrue(success);
		
		BucketProperties readProps = bucketProps.readBucketProperties(propsFile);
		
		assertTrue(readProps != null);
		NamedErlangFunction precommitFn = (NamedErlangFunction)readProps.getPrecommitHooks().toArray()[0];
		assertTrue(precommitFn.getMod().compareTo("named") == 0);
		assertTrue(precommitFn.getFun().compareTo("function") == 0);
		NamedErlangFunction postcommitFn = (NamedErlangFunction)readProps.getPostcommitHooks().toArray()[0];
		assertTrue(postcommitFn.getMod().compareTo("bob") == 0);
		assertTrue(postcommitFn.getFun().compareTo("harry") == 0);
		assertTrue(readProps.getAllowSiblings());
		assertTrue(readProps.getBackend().compareTo("dandb") == 0);
		assertTrue(readProps.getBasicQuorum());
		assertTrue(readProps.getBigVClock() == 1234567890);
		NamedErlangFunction chashFn = readProps.getChashKeyFunction();
		assertTrue(chashFn.getMod().compareTo("sally") == 0);
		assertTrue(chashFn.getFun().compareTo("quenton") == 0);
		assertTrue(readProps.getDW().getIntValue() == 1);
		assertTrue(readProps.getLastWriteWins());
		NamedErlangFunction linkWalkFn = readProps.getLinkWalkFunction();
		assertTrue(linkWalkFn.getMod().compareTo("whos") == 0);
		assertTrue(linkWalkFn.getFun().compareTo("on first") == 0);
		assertTrue(readProps.getNotFoundOK());
		assertTrue(readProps.getNVal() == 2);
		assertTrue(readProps.getOldVClock() == 1234567889);
		assertTrue(readProps.getPR().getIntValue() == 1);
		assertTrue(readProps.getPW().getIntValue() == 1);
		assertTrue(readProps.getR().getIntValue() == 1);
		assertTrue(readProps.getRW().getIntValue() == 1);
		assertTrue(readProps.getSearch());
		assertTrue(readProps.getSmallVClock() == 123456789);
		assertTrue(readProps.getW().getIntValue() == 1);
		assertTrue(readProps.getYoungVClock() == 1234567);
	}

}
