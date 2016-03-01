package net.butfly.albacore.calculus.marshall;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.bson.BSONObject;
import org.bson.LazyBSONCallback;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.jongo.marshall.jackson.bson4jackson.MongoBsonFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.jcabi.log.Logger;
import com.mongodb.BasicDBObject;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.LazyDBObject;

import net.butfly.albacore.calculus.Functor;

public class MongodbMarshaller implements Marshaller<BSONObject, Object> {
	private static ObjectMapper mapper = new ObjectMapper(new MongoBsonFactory());

	@Override
	public <T extends Functor<T>> T unmarshall(BSONObject from, Class<T> to) {
		ObjectReader r = mapper.readerFor(to);
		DBEncoder e = DefaultDBEncoder.FACTORY.create();
		OutputBuffer buf = new BasicOutputBuffer();
		e.writeObject(buf, from);
		try {
			return r.readValue(buf.toByteArray());
		} catch (IOException ex) {
			Logger.error(this.getClass(), "BSON unmarshall failure from " + to.toString(), ex);
			return null;
		}
	}

	@Override
	public <T extends Functor<T>> BSONObject marshall(T from) {
		ObjectWriter w = mapper.writer();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			w.writeValue(baos, from);
		} catch (Exception e) {
			Logger.error(this.getClass(), "BSON marshall failure from " + from.getClass().toString(), e);
			return null;
		}
		DBObject dbo = new LazyDBObject(baos.toByteArray(), new LazyBSONCallback());
		DBObject r = new BasicDBObject();
		r.putAll(dbo);
		return r;
	}

	@Override
	public String unmarshallId(Object id) {
		return id.toString();
	}

	@Override
	public Object marshallId(String id) {
		return id;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public <F extends Functor> void confirm(Class<F> functor) {
		// TODO Auto-generated method stub
	}
}
