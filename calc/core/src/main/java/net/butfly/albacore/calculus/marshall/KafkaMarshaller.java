package net.butfly.albacore.calculus.marshall;

import org.bson.BSON;
import org.bson.BSONObject;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.Detail;

public class KafkaMarshaller extends BsonMarshaller<byte[], String> {
	private static final long serialVersionUID = -4471098188111221100L;

	@Override
	public String unmarshallId(String id) {
		return id;
	}

	@Override
	public String marshallId(String id) {
		return id;
	}

	@Override
	public <F extends Functor<F>> void confirm(Class<F> functor, DataSource ds, Detail detail) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected BSONObject encode(byte[] value) {
		return BSON.decode(value);
	}

	@Override
	protected byte[] decode(BSONObject value) {
		return BSON.encode(value);
	}
}
