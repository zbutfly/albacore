package net.butfly.albacore.serder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.reflect.TypeToken;

public class HbaseValueSerder implements Serder<ByteArrayOutputStream, ImmutableBytesWritable> {
	private static final long serialVersionUID = 1152380944308233135L;

	@Override
	public <T extends ByteArrayOutputStream> ImmutableBytesWritable ser(T from) {
		return null == from ? null : new ImmutableBytesWritable(from.toByteArray());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends ByteArrayOutputStream> T der(ImmutableBytesWritable from, TypeToken<T> to) {
		if (null == from) return null;
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		try {
			bao.write(from.get());
		} catch (IOException e) {
			return null;
		}
		return (T) bao;
	}
}
