package net.butfly.albacore.calculus.datasource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.util.MongoClientURIBuilder;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class MongoDataSource extends DataSource<Object, Object, BSONObject, MongoDataDetail> {
	private static final long serialVersionUID = -2617369621178264387L;
	String uri;

	public MongoDataSource(String uri, MongoMarshaller marshaller) {
		super(Type.MONGODB, null == marshaller ? new MongoMarshaller() : marshaller);
		this.uri = uri;
	}

	public MongoDataSource(String uri) {
		this(uri, new MongoMarshaller());
	}

	@Override
	public String toString() {
		return super.toString() + ":" + this.uri;
	}

	public String getUri() {
		return uri;
	}

	@Override
	public boolean confirm(Class<? extends Factor<?>> factor, MongoDataDetail detail) {
		MongoClientURI muri = new MongoClientURI(getUri());
		MongoClient mclient = new MongoClient(muri);
		try {
			MongoDatabase db = mclient.getDatabase(muri.getDatabase());
			MongoCollection<Document> col = db.getCollection(detail.mongoTable);
			if (col == null) {
				db.createCollection(detail.mongoTable);
				col = db.getCollection(detail.mongoTable);
			}
			for (Field f : Reflections.getDeclaredFields(factor))
				if (f.isAnnotationPresent(Index.class)) {
					String colname = f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
							: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
					BSONObject dbi = new BasicDBObject();
					dbi.put(colname, 1);
					col.createIndex((Bson) dbi);
				}
			return true;
		} finally {
			mclient.close();
		}
	}

	@Override
	public <F extends Factor<F>> JavaPairRDD<Object, F> stocking(Calculator calc, Class<F> factor, MongoDataDetail detail,
			String referField, Set<?> referValues) {
		if (logger.isDebugEnabled()) logger.debug("Stocking begin: " + factor.toString());
		Configuration mconf = new Configuration();
		mconf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
		MongoClientURI uri = new MongoClientURI(this.uri);
		// mconf.set("mongo.auth.uri", uri.toString());
		mconf.set("mongo.input.uri", new MongoClientURIBuilder(uri).collection(uri.getDatabase(), detail.mongoTable).build().toString());
		String qstr = filter(detail.mongoFilter, referField, referValues);
		if (logger.isTraceEnabled()) logger.trace("Run mongodb filter: " + qstr);
		if (null != qstr) mconf.set("mongo.input.query", qstr);
		// conf.mconf.set("mongo.input.fields
		mconf.set("mongo.input.notimeout", "true");
		JavaPairRDD<Object, BSONObject> rr = calc.sc.newAPIHadoopRDD(mconf, MongoInputFormat.class, Object.class, BSONObject.class);
		if (logger.isTraceEnabled()) logger.trace("MongoDB read: " + rr.count());
		return rr.mapToPair(t -> null == t ? null
				: new Tuple2<Object, F>(this.marshaller.unmarshallId(t._1), this.marshaller.unmarshall(t._2, factor)));
	}

	private String filter(String filter, String referField, Set<?> referValues) {
		MongoMarshaller bsoner = (MongoMarshaller) this.marshaller;
		List<BSONObject> qs = new ArrayList<>();
		if (referField != null) {
			BSONObject qq = new BasicDBObject();
			qq.put("$in", referValues.toArray(new String[referValues.size()]));
			BSONObject rq = new BasicDBObject();
			rq.put(referField, qq);
			qs.add(rq);
		}
		if (filter != null) qs.add(bsoner.bsonFromJSON(filter));
		switch (qs.size()) {
		case 0:
			return null;
		case 1:
			return bsoner.jsonFromBSON(qs.get(0));
		default:
			BSONObject q = new BasicDBObject();
			q.put("$and", qs);
			return bsoner.jsonFromBSON(q);
		}
	}

	@Override
	public <F extends Factor<F>> VoidFunction<JavaPairRDD<Object, F>> saving(Calculator calc, MongoDataDetail detail) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("mongo.job.output.format", MongoOutputFormat.class.getName());
		MongoClientURI uri = new MongoClientURI(this.uri);
		conf.set("mongo.output.uri", new MongoClientURIBuilder(uri).collection(uri.getDatabase(), detail.mongoTable).build().toString());
		return r -> {
			r.mapToPair(t -> {
				BasicBSONObject q = new BasicBSONObject();
				q.append("_id", this.marshaller.marshallId(t._1));
				BasicBSONObject u = new BasicBSONObject();
				u.append("$set", this.marshaller.marshall(t._2));
				if (logger.isTraceEnabled()) logger.trace("MongoUpdateWritable: " + u.toString() + " from " + q.toString());
				return new Tuple2<Object, MongoUpdateWritable>(null, new MongoUpdateWritable(q, u, true, true));
			}).saveAsNewAPIHadoopFile("", Object.class, BSONObject.class, MongoOutputFormat.class, conf);
		};
	}
}