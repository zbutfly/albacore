package net.butfly.albacore.calculus.datasource;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.bson.BSONObject;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoClientURIBuilder;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class MongoDataSource extends DataSource<Object, BSONObject, MongoDetail> {
	private static final long serialVersionUID = -2617369621178264387L;
	String uri;

	public MongoDataSource(String uri, Marshaller<Object, BSONObject> marshaller) {
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
	public boolean confirm(Class<? extends Factor<?>> factor, MongoDetail detail) {
		MongoClientURI muri = new MongoClientURI(getUri());
		MongoClient mclient = new MongoClient(muri);
		try {
			@SuppressWarnings("deprecation")
			DB mdb = mclient.getDB(muri.getDatabase());

			if (mdb.collectionExists(detail.mongoTable)) return true;
			DBCollection col = mdb.createCollection(detail.mongoTable, new BasicDBObject());

			for (Field f : Reflections.getDeclaredFields(factor))
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

	@SuppressWarnings("unchecked")
	@Override
	public <K, F extends Factor<F>> JavaPairRDD<K, F> stocking(JavaSparkContext sc, Class<F> factor, MongoDetail detail) {
		Configuration mconf = new Configuration();
		mconf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
		MongoClientURI uri = new MongoClientURI(this.uri);
		// mconf.set("mongo.auth.uri", uri.toString());
		mconf.set("mongo.input.uri", new MongoClientURIBuilder(uri).collection(uri.getDatabase(), detail.mongoTable).build().toString());
		if (detail.mongoFilter != null && !"".equals(detail.mongoFilter)) mconf.set("mongo.input.query", detail.mongoFilter);
		// conf.mconf.set("mongo.input.fields
		mconf.set("mongo.input.notimeout", "true");
		return (JavaPairRDD<K, F>) sc.newAPIHadoopRDD(mconf, MongoInputFormat.class, Object.class, BSONObject.class)
				.mapToPair(t -> null == t ? null
						: new Tuple2<String, F>(this.marshaller.unmarshallId(t._1), this.marshaller.unmarshall(t._2, factor)));
	}

	@Override
	public <K, F extends Factor<F>> VoidFunction<JavaPairRDD<K, F>> saving(JavaSparkContext sc, MongoDetail detail) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("mongo.job.output.format", MongoOutputFormat.class.getName());
		MongoClientURI uri = new MongoClientURI(this.uri);
		conf.set("mongo.output.uri", new MongoClientURIBuilder(uri).collection(uri.getDatabase(), detail.mongoTable).build().toString());
		return r -> r
				.mapToPair(t -> null == t ? null : new Tuple2<>(this.marshaller.marshallId((String) t._1), this.marshaller.marshall(t._2)))
				.saveAsNewAPIHadoopFile("", Object.class, BSONObject.class, MongoOutputFormat.class, conf);
	}
}