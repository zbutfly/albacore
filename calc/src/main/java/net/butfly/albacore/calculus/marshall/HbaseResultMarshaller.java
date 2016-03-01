package net.butfly.albacore.calculus.marshall;

import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;

import com.jcabi.log.Logger;

import net.butfly.albacore.calculus.Functor;

public class HbaseResultMarshaller implements Marshaller<Result, ImmutableBytesWritable> {
	@Override
	public <T extends Functor<T>> T unmarshall(Result from, Class<T> to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Functor<T>> Result marshall(T from) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String unmarshallId(ImmutableBytesWritable id) {
		try {
			return Text.decode(id.get());
		} catch (CharacterCodingException e) {
			Logger.error(this.getClass(), "ImmutableBytesWritable unmarshall failure.", e);
			return null;
		}
	}

	@Override
	public ImmutableBytesWritable marshallId(String id) {
		try {
			return new ImmutableBytesWritable(Text.encode(id).array());
		} catch (CharacterCodingException e) {
			Logger.error(this.getClass(), "ImmutableBytesWritable marshall failure.", e);
			return null;
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public <F extends Functor> void confirm(Class<F> functor) {
		// TODO Auto-generated method stub
	}
}
