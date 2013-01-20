package com.basho.riak.client.raw.pbc;

import static com.basho.riak.client.raw.pbc.ConversionUtil.convert;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.pbc.RiakObject;

public class ConversionUtilWrapper {
	public static IRiakObject convertConcreteToInterface(RiakObject object) {
		return convert(object);
	}
	
	public static RiakObject convertInterfaceToConcrete(IRiakObject object) {
		return convert(object);
	}

}
