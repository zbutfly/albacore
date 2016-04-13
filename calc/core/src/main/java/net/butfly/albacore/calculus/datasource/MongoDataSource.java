package net.butfly.albacore.calculus.datasource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.InputFormat;
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
import com.mongodb.hadoop.splitter.MongoPaginatingSplitter;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import com.mongodb.hadoop.util.MongoConfigUtil;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.Filter;
import net.butfly.albacore.calculus.factor.filter.MongoFilter;
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
		debug(() -> "Stocking begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		Configuration mconf = new Configuration();
		mconf.setClass(MongoConfigUtil.JOB_INPUT_FORMAT, MongoInputFormat.class, InputFormat.class);
		MongoClientURI uri = new MongoClientURI(this.uri);
		// mconf.set(MongoConfigUtil.INPUT_URI, uri.toString());
		mconf.set(MongoConfigUtil.INPUT_URI,
				new MongoClientURIBuilder(uri).collection(uri.getDatabase(), detail.tables[0]).build().toString());
		List<BSONObject> ands = new ArrayList<>();
		if (detail.filter != null) ands.add(((MongoMarshaller) this.marshaller).bsonFromJSON(detail.filter));
		for (Filter f : filters) {
			if (f instanceof Filter.Limit) {
				warn(() -> "MongoDB query limit set as [" + ((Filter.Limit) f).limit + "], maybe debug...");
				mconf.setLong(MongoConfigUtil.INPUT_LIMIT, ((Filter.Limit) f).limit);
			} else if (f instanceof Filter.Skip) {
				warn(() -> "MongoDB query skip set as [" + ((Filter.Skip) f).skip + "], maybe debug...");
				mconf.setLong(MongoConfigUtil.INPUT_SKIP, ((Filter.Skip) f).skip);
			} else if (f instanceof Filter.Sort) {
				warn(() -> "MongoDB query sort set as [" + ((Filter.Sort) f).field + ":" + ((Filter.Sort) f).asc + "], maybe debug...");
				mconf.set(MongoConfigUtil.INPUT_SORT, ((MongoMarshaller) this.marshaller)
						.jsonFromBSON(assembly(((Filter.Sort) f).field, ((Filter.Sort) f).asc ? 1 : -1)));
			} else ands.add(filter(factor, f));
		}
		String inputquery = fromBSON(ands);
		if (null != inputquery) {
			mconf.set(MongoConfigUtil.INPUT_QUERY, inputquery);
			if (this.optimize) {
				mconf.setBoolean(MongoConfigUtil.SPLITS_USE_RANGEQUERY, true);
				mconf.setClass(MongoConfigUtil.MONGO_SPLITTER_CLASS, MongoPaginatingSplitter.class, MongoSplitter.class);
				info(() -> "Use optimized spliter: " + MongoPaginatingSplitter.class.toString());
			}
			trace(() -> "Run mongodb filter on " + factor.toString() + ": "
					+ (inputquery.length() <= 200 || calc.debug ? inputquery
							: inputquery.substring(0, 100) + "...(too long string eliminated)")
					+ (mconf.get(MongoConfigUtil.INPUT_LIMIT) == null ? "" : ", limit: " + mconf.get(MongoConfigUtil.INPUT_LIMIT))
					+ (mconf.get(MongoConfigUtil.INPUT_SKIP) == null ? "" : ", skip: " + mconf.get(MongoConfigUtil.INPUT_SKIP)) + ".");
		}
		// conf.mconf.set(MongoConfigUtil.INPUT_FIELDS
		mconf.setBoolean(MongoConfigUtil.INPUT_NOTIMEOUT, true);
		// mconf.set("mongo.input.split.use_range_queries", "true");

		return calc.sc.newAPIHadoopRDD(mconf, MongoInputFormat.class, Object.class, BSONObject.class)
				.mapToPair(t -> new Tuple2<>(marshaller.unmarshallId(t._1), marshaller.unmarshall(t._2, factor)));
	}

	private String fromBSON(List<BSONObject> ands) {
		switch (ands.size()) {
		case 0:
			return null;
		case 1:
			return ((MongoMarshaller) this.marshaller).jsonFromBSON(ands.get(0));
		default:
			return ((MongoMarshaller) this.marshaller).jsonFromBSON(assembly("$and", ands));
		}
	}

	private static final Map<Class<? extends Filter>, String> ops = new HashMap<>();
	static {
		ops.put(Filter.Equal.class, "$eq");
		ops.put(Filter.LessThan.class, "$lt");
		ops.put(Filter.GreaterThan.class, "$gt");
		ops.put(Filter.LessOrEqual.class, "$lte");
		ops.put(Filter.GreaterOrEqual.class, "$gte");
		ops.put(MongoFilter.Regex.class, "$regex");
		ops.put(MongoFilter.Where.class, "$where");
		ops.put(MongoFilter.Type.class, "$type");
	}

	private BSONObject filter(Class<?> mapperClass, Filter filter) {
		if (filter instanceof Filter.FieldFilter) {
			String col = marshaller.parseField(Reflections.getDeclaredField(mapperClass, ((Filter.FieldFilter<?>) filter).field));
			if (filter instanceof Filter.SingleFieldFilter)
				return assembly(col, assembly(ops.get(filter.getClass()), ((Filter.SingleFieldFilter<?>) filter).value));
			if (filter.getClass().equals(Filter.In.class)) return assembly(col, assembly("$in", ((Filter.In<?>) filter).values));
			if (filter.getClass().equals(MongoFilter.Regex.class))
				return assembly(col, assembly("$regex", ((MongoFilter.Regex) filter).regex));
		} else {
			if (filter.getClass().equals(Filter.And.class)) {
				List<BSONObject> ands = new ArrayList<>();
				for (Filter f : ((Filter.And) filter).filters)
					ands.add(filter(mapperClass, f));
				return assembly("$and", ands);
			} else if (filter.getClass().equals(Filter.Or.class)) {
				List<BSONObject> ors = new ArrayList<>();
				for (Filter f : ((Filter.And) filter).filters)
					ors.add(filter(mapperClass, f));
				return assembly("$or", ors);
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
			trace(() -> "Write back to mongodb: " + r.count() + " records.");
			r.mapToPair(t -> {
				BasicBSONObject q = new BasicBSONObject();
				q.append("_id", this.marshaller.marshallId(t._1));
				BasicBSONObject u = new BasicBSONObject();
				u.append("$set", this.marshaller.marshall(t._2));
//				if (calc.debug) trace(() -> "MongoUpdateWritable: " + u.toString() + " from " + q.toString());
				return new Tuple2<Object, MongoUpdateWritable>(null, new MongoUpdateWritable(q, u, true, true));
			}).saveAsNewAPIHadoopFile("", Object.class, BSONObject.class, MongoOutputFormat.class, conf);
		};
	}
}