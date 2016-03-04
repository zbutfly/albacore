package net.butfly.albacore.calculus.marshall;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;

import org.bson.BSONObject;
import org.bson.LazyBSONCallback;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.jongo.marshall.jackson.bson4jackson.MongoBsonFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.CaseFormat;
import com.jcabi.log.Logger;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.LazyDBObject;

import net.butfly.albacore.calculus.CalculatorConfig;
import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.FunctorConfig;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.MongoDataSource;
import net.butfly.albacore.calculus.datasource.Index;
import net.butfly.albacore.utils.Reflections;

public class MongoMarshaller implements Marshaller<BSONObject, Object> {
	private static final long serialVersionUID = 8467183278278572295L;
	private static ObjectMapper mapper = new ObjectMapper(new MongoBsonFactory());

	@Override
	public <T extends Functor<T>> T unmarshall(BSONObject from, Class<T> to) {
		ObjectReader r = mapper.reader(to);
		DBEncoder e = DefaultDBEncoder.FACTORY.create();
		OutputBuffer buf = new BasicOutputBuffer();
		e.writeObject(buf, from);
		try {
			return r.readValue(buf.toByteArray());
		} catch (IOException ex) {
			Logger.error(MongoMarshaller.class, "BSON unmarshall failure from " + to.toString(), ex);
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
			Logger.error(MongoMarshaller.class, "BSON marshall failure from " + from.getClass().toString(), e);
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

	@Override
	public <F extends Functor<F>> void confirm(Class<F> functor, FunctorConfig config, CalculatorConfig globalConfig) {
		// jongo has confirm or create collection on getCollection, so we need
		// only to create index.
		DB db = ((MongoDataSource) globalConfig.datasources.get(config.datasource)).mongo;
		if (db.collectionExists(config.mongoTable)) return;
		DBCollection col = db.createCollection(config.mongoTable, new BasicDBObject());

		for (Field f : Reflections.getDeclaredFields(functor))
			if (f.isAnnotationPresent(Index.class)) {
				String colname = f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
						: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
				DBObject dbi = new BasicDBObject();
				dbi.put(colname, 1);
				col.createIndex(dbi);
			}
	}
};