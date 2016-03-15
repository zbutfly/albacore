package net.butfly.albacore.calculus.marshall.bson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.bson.BSONObject;
import org.bson.LazyBSONCallback;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcabi.log.Logger;
import com.mongodb.BasicDBObject;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.LazyDBObject;

import net.butfly.albacore.calculus.functor.Functor;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;

public abstract class BsonMarshaller<V, K> implements Marshaller<V, K> {
	private static final long serialVersionUID = -7385678674433019238L;
	private static ObjectMapper bsoner = new ObjectMapper(MongoBsonFactory.createFactory())
			.setPropertyNamingStrategy(new UpperCaseWithUnderscoresStrategy()).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
	protected static DBEncoder e = DefaultDBEncoder.FACTORY.create();

	@Override
	public final <T extends Functor<T>> T unmarshall(V from, Class<T> to) {
		if (null == from) return null;
		return unmarshallFromBSON(encode(from), to);

	}

	@Override
	public final <T extends Functor<T>> V marshall(T from) {
		if (null == from) return null;
		return decode(marshallToBSON(from));
	}

	abstract protected BSONObject encode(V value);

	abstract protected V decode(BSONObject value);

	private <T extends Functor<T>> T unmarshallFromBSON(BSONObject bson, Class<T> to) {
		OutputBuffer buf = new BasicOutputBuffer();
		try {
			try {
				// TODO: e
				DefaultDBEncoder.FACTORY.create().writeObject(buf, bson);
			} catch (Exception ex) {
				Logger.error(MongoMarshaller.class, "BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
			try {
				return bsoner.reader(to).readValue(buf.toByteArray());
			} catch (IOException ex) {
				Logger.error(MongoMarshaller.class, "BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
		} finally {
			buf.close();
		}
	}

	private <T extends Functor<T>> BSONObject marshallToBSON(T from) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			bsoner.writer().writeValue(baos, from);
		} catch (IOException e) {
			Logger.error(MongoMarshaller.class, "BSON marshall failure from " + from.getClass().toString(), e);
			return null;
		}
		DBObject r = new BasicDBObject();
		r.putAll(new LazyDBObject(baos.toByteArray(), new LazyBSONCallback()));
		return r;
	}
}
