package net.butfly.albacore.serder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public final class HbaseValueSerder implements Serder<ByteArrayOutputStream, ImmutableBytesWritable> {
	private static final long serialVersionUID = 1152380944308233135L;
	public static final HbaseValueSerder DEFAULT = new HbaseValueSerder();

	@Override
	public ImmutableBytesWritable ser(ByteArrayOutputStream from) {
		return null == from ? null : new ImmutableBytesWritable(from.toByteArray());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends ByteArrayOutputStream> T der(ImmutableBytesWritable from, Class<T> to) {
		try (ByteArrayOutputStream bao = new ByteArrayOutputStream();) {
			bao.write(from.get());
			return (T) bao;
		} catch (IOException e) {
			return null;
		}
	}
}
