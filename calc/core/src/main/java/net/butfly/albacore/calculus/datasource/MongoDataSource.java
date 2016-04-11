package net.butfly.albacore.calculus.datasource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaPairRDD;
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
import com.mongodb.hadoop.util.MongoConfigUtil;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.Filter;
import net.butfly.albacore.calculus.factor.filter.Filter.Between;
import net.butfly.albacore.calculus.factor.filter.Filter.Equal;
import net.butfly.albacore.calculus.factor.filter.Filter.FieldFilter;
import net.butfly.albacore.calculus.factor.filter.Filter.In;
import net.butfly.albacore.calculus.lambda.VoidFunction;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class MongoDataSource extends DataSource<Object, Object, BSONObject, MongoDataDetail> {
	private static final long serialVersionUID = -2617369621178264387L;
	public String uri;
	private boolean optimize;

	public MongoDataSource(String uri, MongoMarshaller marshaller, String suffix, boolean validate, boolean optimize) {
		super(Type.MONGODB, validate, null == marshaller ? new MongoMarshaller() : marshaller);
		super.suffix = suffix;
		this.uri = uri;
		this.optimize = optimize;
	}

	public MongoDataSource(String uri, String suffix, boolean validate, boolean optimize) {
		this(uri, new MongoMarshaller(), suffix, validate, optimize);
	}

	@Override
	public String toString() {
		return super.toString() + ":" + this.uri;
	}

	public String getUri() {
		return uri;
	}

	@SuppressWarnings("deprecation")
	@Override
	public boolean confirm(Class<? extends Factor<?>> factor, MongoDataDetail detail) {
		MongoClientURI muri = new MongoClientURI(getUri());
		MongoClient mclient = new MongoClient(muri);
		try {
			if (!mclient.getDB(muri.getDatabase()).collectionExists(detail.tables[0])) {
				MongoDatabase db = mclient.getDatabase(muri.getDatabase());
				db.createCollection(detail.tables[0]);
				MongoCollection<Document> col = db.getCollection(detail.tables[0]);
				for (Field f : Reflections.getDeclaredFields(factor))
					if (f.isAnnotationPresent(Index.class)) {
						String colname = f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
								: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
						BSONObject dbi = new BasicDBObject();
						dbi.put(colname, 1);
						col.createIndex((Bson) dbi);
					}
			}
			return true;
		} finally {
			mclient.close();
		}
	}

	@Override
	public <F extends Factor<F>> JavaPairRDD<Object, F> stocking(Calculator calc, Class<F> factor, MongoDataDetail detail,
			Filter... filters) {
		if (logger.isDebugEnabled()) logger.debug("Stocking begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		Configuration mconf = new Configuration();
		mconf.set(MongoConfigUtil.JOB_INPUT_FORMAT, "com.mongodb.hadoop.MongoInputFormat");
		MongoClientURI uri = new MongoClientURI(this.uri);
		// mconf.set(MongoConfigUtil.INPUT_URI, uri.toString());
		mconf.set(MongoConfigUtil.INPUT_URI,
				new MongoClientURIBuilder(uri).collection(uri.getDatabase(), detail.tables[0]).build().toString());
		List<BSONObject> ands = new ArrayList<>();
		MongoMarshaller bsoner = (MongoMarshaller) this.marshaller;
		if (detail.filter != null) ands.add(bsoner.bsonFromJSON(detail.filter));
		for (Filter f : filters)
			ands.add(filter(factor, f));
		String inputquery = null;
		switch (ands.size()) {
		case 0:
			break;
		case 1:
			inputquery = bsoner.jsonFromBSON(ands.get(0));
			break;
		default:
			inputquery = bsoner.jsonFromBSON(assembly("$and", ands));
			break;
		}
		if (null != inputquery) {
			mconf.set(MongoConfigUtil.INPUT_QUERY, inputquery);
			if (this.optimize) {
				mconf.set(MongoConfigUtil.SPLITS_USE_RANGEQUERY, "true");
				mconf.set(MongoConfigUtil.MONGO_SPLITTER_CLASS, "com.mongodb.hadoop.splitter.MongoPaginatingSplitter");
			}
			if (logger.isTraceEnabled()) logger.trace("Run mongodb filter on " + factor.toString() + ": "
					+ (inputquery.length() <= 100 ? inputquery : inputquery.substring(0, 100) + "...(too long string eliminated)"));
		}
		// conf.mconf.set(MongoConfigUtil.INPUT_FIELDS
		mconf.set(MongoConfigUtil.INPUT_NOTIMEOUT, "true");
		// mconf.set("mongo.input.split.use_range_queries", "true");

		return calc.sc.newAPIHadoopRDD(mconf, MongoInputFormat.class, Object.class, BSONObject.class)
				.mapToPair(t -> new Tuple2<>(marshaller.unmarshallId(t._1), marshaller.unmarshall(t._2, factor)));
	}

	@SuppressWarnings("rawtypes")
	private BSONObject filter(Class<?> mapperClass, Filter filter) {
		if (filter instanceof FieldFilter) {
			String col = marshaller.parseField(Reflections.getDeclaredField(mapperClass, ((FieldFilter<?>) filter).field));
			if (filter.getClass().equals(In.class)) return assembly(col, assembly("$in", ((In<?>) filter).values));
			if (filter.getClass().equals(Equal.class)) return assembly(col, assembly("$eq", ((Equal<?>) filter).value));
			if (filter.getClass().equals(Between.class)) {
				Filter.Between be = (Between) filter;
				List<BSONObject> ands = new ArrayList<>();
				if (be.min != null) ands.add(assembly(col, assembly("$gte", be.min)));
				if (be.max != null) ands.add(assembly(col, assembly("$lte", be.max)));
				return assembly("$and", ands);
			}
		}
		throw new UnsupportedOperationException("Unsupportted filter: " + filter.getClass());
	}

	private BSONObject assembly(String key, Object value) {
		BSONObject fd = new BasicDBObject();
		fd.put(key, value);
		return fd;
	}

	@Override
	public <F extends Factor<F>> VoidFunction<JavaPairRDD<Object, F>> saving(Calculator calc, MongoDataDetail detail) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("mongo.job.output.format", MongoOutputFormat.class.getName());
		MongoClientURI uri = new MongoClientURI(this.uri);
		conf.set("mongo.output.uri", new MongoClientURIBuilder(uri).collection(uri.getDatabase(), detail.tables[0]).build().toString());
		return r -> {
			if (logger.isTraceEnabled()) {
				r = r.cache();
				logger.trace("Write back to mongodb: " + r.count() + " records.");
			}
			r.mapToPair(t -> {
				BasicBSONObject q = new BasicBSONObject();
				q.append("_id", this.marshaller.marshallId(t._1));
				BasicBSONObject u = new BasicBSONObject();
				u.append("$set", this.marshaller.marshall(t._2));
				if (calc.debug && logger.isTraceEnabled()) logger.trace("MongoUpdateWritable: " + u.toString() + " from " + q.toString());
				return new Tuple2<Object, MongoUpdateWritable>(null, new MongoUpdateWritable(q, u, true, true));
			}).saveAsNewAPIHadoopFile("", Object.class, BSONObject.class, MongoOutputFormat.class, conf);
		};
	}
}