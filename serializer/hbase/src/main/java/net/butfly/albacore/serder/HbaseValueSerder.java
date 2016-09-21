package net.butfly.albacore.serder;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.reflect.TypeToken;

public class HbaseValueSerder implements Serder<byte[], ImmutableBytesWritable> {
	private static final long serialVersionUID = 1152380944308233135L;

	@Override
	public <T> ImmutableBytesWritable ser(T from) {
		return null == from ? null : new ImmutableBytesWritable((byte[]) from);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T der(ImmutableBytesWritable from, TypeToken<T> to) {
		return null == from ? null : (T) from.get();
	}
}
