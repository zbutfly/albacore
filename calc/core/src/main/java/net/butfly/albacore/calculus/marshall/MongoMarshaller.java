package net.butfly.albacore.calculus.marshall;

import java.lang.reflect.Field;

import org.bson.BSONObject;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.MongoDataSource;
import net.butfly.albacore.calculus.datasource.Detail;
import net.butfly.albacore.calculus.datasource.Index;
import net.butfly.albacore.calculus.functor.Functor;
import net.butfly.albacore.calculus.marshall.bson.BsonMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;

public class MongoMarshaller extends BsonMarshaller<BSONObject, Object> {
	private static final long serialVersionUID = 8467183278278572295L;

	@Override
	public String unmarshallId(Object id) {
		return null == id ? null : id.toString();
	}

	@Override
	public Object marshallId(String id) {
		return id;
	}

	@Override
	protected BSONObject encode(BSONObject value) {
		return value;
	}

	@Override
	protected BSONObject decode(BSONObject value) {
		return value;
	}

	@Override
	public <F extends Functor<F>> boolean confirm(Class<F> functor, DataSource ds, Detail detail) {
		// TODO Auto-generated method stub
		MongoDataSource mds = (MongoDataSource) ds;
		MongoClientURI muri = new MongoClientURI(mds.getUri());
		MongoClient mclient = new MongoClient(muri);
		try {
			@SuppressWarnings("deprecation")
			DB mdb = mclient.getDB(muri.getDatabase());

			if (mdb.collectionExists(detail.mongoTable)) return true;
			DBCollection col = mdb.createCollection(detail.mongoTable, new BasicDBObject());

			for (Field f : Reflections.getDeclaredFields(functor))
				if (f.isAnnotationPresent(Index.class)) {
					String colname = f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
							: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
					DBObject dbi = new BasicDBObject();
					dbi.put(colname, 1);
					col.createIndex(dbi);
				}
			return true;
		} finally {
			mclient.close();
		}
	}
};