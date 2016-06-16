package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.util.SizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.modifier.Id;
import net.butfly.albacore.calculus.factor.modifier.Key;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.utils.Logable;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

@SuppressWarnings("rawtypes")
public abstract class DataSource<K, RK, RV, WK, WV> implements Serializable, Logable {
	private static final long serialVersionUID = -1L;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected final Factor.Type type;
	protected final Marshaller<K, RK, RV> marshaller;
	public final Class<RK> keyClass;
	public final Class<RV> valueClass;
	public final Class<? extends OutputFormat> outputFormatClass;
	private final Class<? extends InputFormat<RK, RV>> inputFormatClass;
	protected Configuration outputConfig;

	public final boolean validate;
	public String suffix;

	// debug variables
	public int debugLimit;
	public float debugRandomChance;

	public Factor.Type type() {
		return type;
	}

	public Marshaller<K, RK, RV> marshaller() {
		return marshaller;
	}

	public DataSource(Type type, boolean validate, Marshaller<K, RK, RV> marshaller, Class<RK> keyClass, Class<RV> valueClass,
			Class<? extends OutputFormat> outputFormatClass, Class<? extends InputFormat<RK, RV>> inputFormatClass) {
		super();
		this.type = type;
		this.validate = validate;
		this.marshaller = marshaller;
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.outputFormatClass = outputFormatClass;
		this.inputFormatClass = inputFormatClass;
	}

	@Override
	public String toString() {
		return "CalculatorDataSource:" + this.type;
	}

	public <F extends Factor<F>> JavaPairRDD<K, F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail, float expandPartitions,
			FactorFilter... filters) {
		throw new UnsupportedOperationException("Unsupportted stocking mode: " + type + " on " + factor.toString());
	}

	@Deprecated
	public <F extends Factor<F>> JavaPairRDD<K, F> batching(Calculator calc, Class<F> factorClass, long batching, K offset,
			DataDetail<F> detail, FactorFilter... filters) {
		throw new UnsupportedOperationException("Unsupportted stocking mode with batching: " + type + " on " + factorClass.toString());
	}

	public <F extends Factor<F>> JavaPairDStream<K, F> streaming(Calculator calc, Class<F> factor, DataDetail<F> detail,
			FactorFilter... filters) {
		throw new UnsupportedOperationException("Unsupportted streaming mode: " + type + " on " + factor.toString());
	}

	public <F> boolean confirm(Class<F> factor, DataDetail<F> detail) {
		return true;
	}

	public static class DataSources extends HashMap<String, DataSource> {
		private static final long serialVersionUID = -7809799411800022817L;

		@SuppressWarnings("unchecked")
		public <DS extends DataSource> DS ds(String dbid) {
			return (DS) super.get(dbid);
		}
	}

	public <V> Tuple2<WK, WV> beforeWriting(K key, V value) {
		throw new UnsupportedOperationException("Unsupportted saving prepare: " + type);
	}

	protected FactorFilter[] adddebug(FactorFilter[] filters) {
		List<FactorFilter> l = new ArrayList<>(Arrays.asList(filters));
		if (debugRandomChance > 0) {
			error(() -> "DataSource [" + type + "] debugging, sampling results of chance: " + debugRandomChance);
			l.add(new FactorFilter.Random(debugRandomChance));
		}
		if (debugLimit > 0) {
			error(() -> "DataSource [" + type + "] debugging, limiting results in: " + debugLimit);
			l.add(new FactorFilter.Limit(debugLimit));
		}
		return l.toArray(new FactorFilter[l.size()]);
	}

	public void save(JavaPairRDD<WK, WV> rdd, DataDetail<?> dd) {
		trace(() -> "Writing to " + type + ": " + rdd.count());
		rdd.saveAsNewAPIHadoopFile("", keyClass, valueClass, outputFormatClass, dd.outputConfiguration(this));
	}

	@SuppressWarnings("unchecked")
	protected <F extends Factor<F>> JavaPairRDD<K, F> readByInputFormat(JavaSparkContext sc, Configuration conf, Class<F> factor,
			float expandPartitions) {
		JavaPairRDD<RK, RV> raw = sc.newAPIHadoopRDD(conf, inputFormatClass, keyClass, valueClass);
		debug(()->"Loading from datasource finished: " + SizeEstimator.estimate(raw) + " bytes (estimate).");
		Set<Field> ids = marshaller.parseAll(factor, Id.class).keySet();
		if (ids.size() > 1) error(() -> "Multiple @Id on " + factor.toString() + ", only use one (but randomized one).");
		final String id = ids.isEmpty() ? null : new ArrayList<>(ids).get(0).getName();
		Set<Field> keys = marshaller.parseAll(factor, Key.class).keySet();
		if (keys.size() > 1) error(() -> "Multiple @Key on " + factor.toString() + ", only use one (but randomized one).");
		final String key = keys.isEmpty() ? null : new ArrayList<>(keys).get(0).getName();
		if (null != key && null == id)
			throw new IllegalArgumentException("@Key defined but @Id not defined on " + factor.toString() + ", id will lose in mapping.");
		final JavaPairRDD<K, F> results = raw.mapToPair(t -> {
			F v = marshaller.unmarshall(t._2, factor);
			K k = null == key ? marshaller.unmarshallId(t._1) : Reflections.get(v, key);
			if (null != id) Reflections.set(v, id, marshaller.unmarshallId(t._1));;
			return new Tuple2<>(k, v);
		});
		return (expandPartitions > 1) ? results.repartition((int) Math.ceil(results.getNumPartitions() * expandPartitions)) : results;
	}
}
