package com.basho.proserv.datamigrator.riak;

import java.util.Map;
import java.util.Set;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;

public class BucketRenamer {
	public static IRiakObject renameBucket(IRiakObject riakObject, String bucketName) {
		RiakObjectBuilder builder = RiakObjectBuilder.newBuilder(bucketName, riakObject.getKey());
		Map<BinIndex, Set<String>> binIndices = riakObject.allBinIndexes(); 
		for (BinIndex index : binIndices.keySet()) {
			for (String value : binIndices.get(index)) {
				builder.addIndex(index.getName(), value);
			}
		}
		Map<IntIndex, Set<Long>> intIndices = riakObject.allIntIndexesV2();
		for (IntIndex index : intIndices.keySet()) {
			for (long value : intIndices.get(index)) {
				builder.addIndex(index.getName(), value);
			}
		}
		for (RiakLink link : riakObject.getLinks()) {
			builder.addLink(link.getBucket(), link.getKey(), link.getTag());
		}
		Map<String, String> userMeta = riakObject.getMeta();
		for (String key : userMeta.keySet()) {
			builder.addUsermeta(key, userMeta.get(key));
		}
		builder.withContentType(riakObject.getContentType());
		// skip lastModified
		builder.withValue(riakObject.getValue());
		builder.withVClock(riakObject.getVClock().getBytes());
		// skip vtag
		
		return builder.build();
	}
}
