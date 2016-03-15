package net.butfly.albacore.calculus.marshall.bson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.bson.BSONObject;
import org.bson.LazyBSONCallback;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.jcabi.log.Logger;
import com.mongodb.BasicDBObject;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.LazyDBObject;

import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonParser;
import net.butfly.albacore.calculus.functor.Functor;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;

public abstract class BsonMarshaller<V, K> implements Marshaller<V, K> {
	private static final long serialVersionUID = -7385678674433019238L;
	protected static BsonFactory factory = new BsonFactory().enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH);
	protected static ObjectMapper mapper = new ObjectMapper(new MongoBsonFactory())
			.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

	@Override
	public <T extends Functor<T>> T unmarshall(V from, Class<T> to) {
		ObjectReader r = mapper.reader(to);
		DBEncoder e = DefaultDBEncoder.FACTORY.create();
		OutputBuffer buf = new BasicOutputBuffer();
		e.writeObject(buf, encode(from));
		try {
			return r.readValue(buf.toByteArray());
		} catch (IOException ex) {
			Logger.error(MongoMarshaller.class, "BSON unmarshall failure from " + to.toString(), ex);
			return null;
		}
	}

	@Override
	public <T extends Functor<T>> V marshall(T from) {
		ObjectWriter w = mapper.writer();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			w.writeValue(baos, from);
		} catch (Exception e) {
			Logger.error(MongoMarshaller.class, "BSON marshall failure from " + from.getClass().toString(), e);
			return null;
		}
		DBObject dbo = new LazyDBObject(baos.toByteArray(), new LazyBSONCallback());
		DBObject r = new BasicDBObject();
		r.putAll(dbo);
		return decode(r);
	}

	abstract protected BSONObject encode(V value);

	abstract protected V decode(BSONObject value);
}
