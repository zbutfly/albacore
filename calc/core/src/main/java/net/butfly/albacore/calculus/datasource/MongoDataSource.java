package net.butfly.albacore.calculus.datasource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

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
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

@SuppressWarnings("rawtypes")
public class MongoDataSource extends DataSource<Object, Object, BSONObject, ObjectId, MongoUpdateWritable> {
	private static final long serialVersionUID = -2617369621178264387L;
	public String uri;
	private boolean optimize;

	public MongoDataSource(String uri, MongoMarshaller marshaller, String suffix, boolean validate, boolean optimize) {
		super(Type.MONGODB, validate, null == marshaller ? new MongoMarshaller() : marshaller, Object.class, BSONObject.class,
				MongoOutputFormat.class);
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

	@SuppressWarnings({ "deprecation" })
	@Override
	public <F> boolean confirm(Class<F> factor, DataDetail<F> detail) {
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
	public <F extends Factor<F>> JavaPairRDD<Object, F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail,
			FactorFilter... filters) {
		debug(() -> "Stocking begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		Configuration mconf = new Configuration();
		mconf.setClass(MongoConfigUtil.JOB_INPUT_FORMAT, MongoInputFormat.class, InputFormat.class);
		MongoClientURI uri = new MongoClientURI(this.uri);
		// mconf.set(MongoConfigUtil.INPUT_URI, uri.toString());
		mconf.set(MongoConfigUtil.INPUT_URI,
				new MongoClientURIBuilder(uri).collection(uri.getDatabase(), detail.tables[0]).build().toString());
		List<BSONObject> ands = new ArrayList<>();
		if (detail.filter != null) ands.add(((MongoMarshaller) this.marshaller).bsonFromJSON(detail.filter));
		for (FactorFilter f : filters) {
			if (null == f) continue;
			if (f instanceof FactorFilter.Limit) {
				warn(() -> "MongoDB query limit set as [" + ((FactorFilter.Limit) f).limit + "], maybe debug...");
				mconf.setLong(MongoConfigUtil.INPUT_LIMIT, ((FactorFilter.Limit) f).limit);
			} else if (f instanceof FactorFilter.Skip) {
				warn(() -> "MongoDB query skip set as [" + ((FactorFilter.Skip) f).skip + "], maybe debug...");
				mconf.setLong(MongoConfigUtil.INPUT_SKIP, ((FactorFilter.Skip) f).skip);
			} else if (f instanceof FactorFilter.Sort) {
				warn(() -> "MongoDB query sort set as [" + ((FactorFilter.Sort) f).field + ":" + ((FactorFilter.Sort) f).asc
						+ "], maybe debug...");
				mconf.set(MongoConfigUtil.INPUT_SORT, ((MongoMarshaller) this.marshaller)
						.jsonFromBSON(assembly(((FactorFilter.Sort) f).field, ((FactorFilter.Sort) f).asc ? 1 : -1)));
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
				.mapToPair(new PairFunction<Tuple2<Object, BSONObject>, Object, F>() {
					@Override
					public Tuple2<Object, F> call(Tuple2<Object, BSONObject> t) throws Exception {
						return new Tuple2<>(marshaller.unmarshallId(t._1), marshaller.unmarshall(t._2, factor));
					}
				});
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

	private static final Map<Class<? extends FactorFilter>, String> ops = new HashMap<>();
	static {
		ops.put(FactorFilter.Equal.class, "$eq");
		ops.put(FactorFilter.NotEqual.class, "$ne");
		ops.put(FactorFilter.Less.class, "$lt");
		ops.put(FactorFilter.Greater.class, "$gt");
		ops.put(FactorFilter.LessOrEqual.class, "$lte");
		ops.put(FactorFilter.GreaterOrEqual.class, "$gte");
		ops.put(FactorFilter.Regex.class, "$regex");
		ops.put(FactorFilter.Where.class, "$where");
		ops.put(FactorFilter.Type.class, "$type");
	}

	private BSONObject filter(Class<?> mapperClass, FactorFilter filter) {
		if (filter instanceof FactorFilter.ByField) {
			String col = marshaller.parseField(Reflections.getDeclaredField(mapperClass, ((FactorFilter.ByField<?>) filter).field));
			if (filter instanceof FactorFilter.ByFieldValue)
				return assembly(col, assembly(ops.get(filter.getClass()), ((FactorFilter.ByFieldValue<?>) filter).value));
			if (filter.getClass().equals(FactorFilter.In.class))
				return assembly(col, assembly("$in", ((FactorFilter.In<?>) filter).values));
			if (filter.getClass().equals(FactorFilter.Regex.class))
				return assembly(col, assembly("$regex", ((FactorFilter.Regex) filter).regex));
		} else {
			if (filter.getClass().equals(FactorFilter.And.class)) {
				List<BSONObject> ands = new ArrayList<>();
				for (FactorFilter f : ((FactorFilter.And) filter).filters)
					ands.add(filter(mapperClass, f));
				return assembly("$and", ands);
			} else if (filter.getClass().equals(FactorFilter.Or.class)) {
				List<BSONObject> ors = new ArrayList<>();
				for (FactorFilter f : ((FactorFilter.And) filter).filters)
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

	@SuppressWarnings("unchecked")
	@Override
	public <F> Tuple2<ObjectId, MongoUpdateWritable> prepare(Object key, F factor, Class<F> factorClass) throws Exception {
		final BasicBSONObject q = new BasicBSONObject();
		q.append("_id", this.marshaller.marshallId(key));
		final BasicBSONObject u = new BasicBSONObject();
		u.append("$set", marshaller.marshall((Factor) factor));
		return new Tuple2<ObjectId, MongoUpdateWritable>(new ObjectId(), new MongoUpdateWritable(q, u, true, true));
	}
}