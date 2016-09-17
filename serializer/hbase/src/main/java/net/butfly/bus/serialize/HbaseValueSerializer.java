package net.butfly.bus.serialize;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import net.butfly.albacore.serializer.ConfirmSerializer;

public class HbaseValueSerializer implements ConfirmSerializer<byte[], ImmutableBytesWritable> {
	private static final long serialVersionUID = 1152380944308233135L;

	@Override
	public byte[] deserialize(ImmutableBytesWritable id, Class<byte[]> to) {
		return null == id ? null : id.get();
	}

	@Override
	public ImmutableBytesWritable serialize(byte[] bytes) {
		return null == bytes ? null : new ImmutableBytesWritable(bytes);
	}
}
