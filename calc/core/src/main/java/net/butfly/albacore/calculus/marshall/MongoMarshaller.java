package net.butfly.albacore.calculus.marshall;

import java.lang.reflect.Field;

import org.bson.BSONObject;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.datasource.DataContext.MongoContext;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.Detail;
import net.butfly.albacore.calculus.datasource.Index;
import net.butfly.albacore.calculus.utils.Reflections;

public class MongoMarshaller extends BsonMarshaller<BSONObject, Object> {
	private static final long serialVersionUID = 8467183278278572295L;

	@Override
	public String unmarshallId(Object id) {
		return id.toString();
	}

	@Override
	public Object marshallId(String id) {
		return id;
	}

	@Override
	public <F extends Functor<F>> void confirm(Class<F> functor, DataSource ds, Detail detail) {
		// jongo has confirm or create collection on getCollection, so we need
		// only to create index.
		MongoContext dc = new MongoContext(ds);
		DB db = dc.getMongo();
		if (db.collectionExists(detail.mongoTable)) return;
		DBCollection col = db.createCollection(detail.mongoTable, new BasicDBObject());

		for (Field f : Reflections.getDeclaredFields(functor))
			if (f.isAnnotationPresent(Index.class)) {
				String colname = f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
						: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
				DBObject dbi = new BasicDBObject();
				dbi.put(colname, 1);
				col.createIndex(dbi);
			}
	}

	@Override
	protected BSONObject encode(BSONObject value) {
		return value;
	}

	@Override
	protected BSONObject decode(BSONObject value) {
		return value;
	}
};