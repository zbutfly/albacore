package net.butfly.bus.serialize;

import java.util.function.Function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import net.butfly.albacore.serder.Serder;
import net.butfly.albacore.serder.SerderBase;

public class HbaseValueSerder extends SerderBase<byte[], ImmutableBytesWritable> implements Serder<byte[], ImmutableBytesWritable> {
	private static final long serialVersionUID = 1152380944308233135L;

	public HbaseValueSerder(Function<String, String> mapping) {
		super(mapping);
	}

	@Override
	public ImmutableBytesWritable serialize(Object from) {
		return null == from ? null : new ImmutableBytesWritable((byte[]) from);
	}

	@Override
	public Object deserialize(ImmutableBytesWritable from, Class<?> to) {
		return deserializeT(from, byte[].class);
	}
}
