package net.butfly.albacore.calculus.factor;

import java.util.HashMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.BSONObject;

import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.DataSources;
import net.butfly.albacore.calculus.datasource.HbaseDataDetail;
import net.butfly.albacore.calculus.datasource.KafkaDataDetail;
import net.butfly.albacore.calculus.datasource.MongoDataDetail;
import net.butfly.albacore.calculus.factor.Factor.Stocking;
import net.butfly.albacore.calculus.factor.Factor.Streaming;
import net.butfly.albacore.calculus.factor.Factoring.OnStreaming;
import net.butfly.albacore.calculus.streaming.JavaConstPairDStream;
import net.butfly.albacore.calculus.streaming.JavaFreshPairDStream;

@SuppressWarnings("unchecked")
public final class Factors extends HashMap<String, JavaPairInputDStream<?, ? extends Factor<?>>> {
	private static final long serialVersionUID = -3712903710207597570L;
	private JavaStreamingContext ssc;
	private DataSources dss;

	public Factors(JavaStreamingContext ssc, Mode mode, DataSources dss, boolean validate, Factoring... factoring) {
		super(factoring.length);
		this.ssc = ssc;
		this.dss = dss;

		FactorConfig<?, ?> batch = null;
		for (Factoring f : factoring) {
			if (mode == Mode.STREAMING && f.streaming() == OnStreaming.NONE) continue;
			@SuppressWarnings("rawtypes")
			Class fc = f.factor();
			if (!fc.isAnnotationPresent(Stocking.class) && !fc.isAnnotationPresent(Streaming.class)) continue;
			FactorConfig<?, ?> c = scan(mode, fc, dss, validate);
			c.batching = f.batching();
			c.streaming = f.streaming();
			if (f.batching() > 0) {
				if (batch != null) throw new IllegalArgumentException("Only one batch stocking source supported, now found second: "
						+ batch.factorClass.toString() + " and " + c.factorClass.toString());
				else batch = c;
			}
			read(mode, f.key(), c);
		}
	}

	public <K, F extends Factor<F>> void streaming(String factoring, JavaPairInputDStream<K, F> ds) {
		if (this.containsKey(factoring)) throw new IllegalArgumentException("Conflictted factoring id: " + factoring);
		this.put(factoring, ds);
	}

	public <K, F extends Factor<F>> void stocking(String factoring, JavaPairRDD<K, F> rdd, JavaStreamingContext ssc) {
		if (this.containsKey(factoring)) throw new IllegalArgumentException("Conflictted factoring id: " + factoring);
		rdd.setName("RDD [" + factoring + "]");
		this.put(factoring, new JavaConstPairDStream<>(ssc, rdd));
	}

	public <K, F extends Factor<F>> JavaPairInputDStream<K, F> streaming(String factoring) {
		return (JavaPairInputDStream<K, F>) get(factoring);
	}

	public <K, F extends Factor<F>> JavaPairInputDStream<K, F> stocking(String factoring) {
		return (JavaPairInputDStream<K, F>) get(factoring);
	}

	private <K, F extends Factor<F>> void read(Mode mode, String key, FactorConfig<K, F> config) {
		switch (mode) {
		case STOCKING:
			if (config.batching <= 0)
				this.stocking(key, dss.ds(config.dbid).stocking(ssc.sparkContext(), config.factorClass, config.detail), ssc);
			else this.streaming(key, dss.ds(config.dbid).batching(ssc, config.factorClass, config.batching, config.detail, config.keyClass,
					config.factorClass));
			break;
		case STREAMING:
			switch (config.mode) {
			case STOCKING:
				switch (config.streaming) {
				case NONE:
					break;
				case ONCE:
					this.streaming(key, (JavaPairInputDStream<K, F>) new JavaConstPairDStream<K, F>(ssc,
							dss.ds(config.dbid).stocking(ssc.sparkContext(), config.factorClass, config.detail)).persist());
					break;
				case EACH:
					this.streaming(key,
							new JavaFreshPairDStream<K, F>(ssc,
									() -> dss.ds(config.dbid).stocking(ssc.sparkContext(), config.factorClass, config.detail),
									config.keyClass, config.factorClass));
					break;
				case CACHE:
					throw new NotImplementedException();
				}
				break;
			case STREAMING:
				this.streaming(key, dss.ds(config.dbid).streaming(ssc, config.factorClass, config.detail));
				break;
			}
			break;
		}
	}

	public static <K, F extends Factor<F>> FactorConfig<K, F> scan(Mode mode, Class<F> factor, DataSources dss, boolean validate) {
		FactorConfig<K, F> config = new FactorConfig<K, F>();
		config.factorClass = factor;
		if (mode == Mode.STREAMING && factor.isAnnotationPresent(Streaming.class)) {
			config.mode = Mode.STREAMING;
			Streaming s = factor.getAnnotation(Streaming.class);
			config.dbid = s.source();
			switch (s.type()) {
			case KAFKA:
				config.detail = new KafkaDataDetail(s.table());
				break;
			default:
				throw new UnsupportedOperationException("Unsupportted streaming mode: " + s.type() + " on " + factor.toString());
			}
		} else {
			Stocking s = factor.getAnnotation(Stocking.class);
			config.mode = Mode.STOCKING;
			config.dbid = s.source();
			switch (s.type()) {
			case HBASE:
				if (Factor.NOT_DEFINED.equals(s.table()))
					throw new IllegalArgumentException("Table not defined for factor " + factor.toString());
				config.detail = new HbaseDataDetail(s.table());
				if (validate) {
					DataSource<ImmutableBytesWritable, Result, HbaseDataDetail> hds = dss.ds(s.source());
					hds.confirm(factor, (HbaseDataDetail) config.detail);
				}
				break;
			case MONGODB:
				if (Factor.NOT_DEFINED.equals(s.table()))
					throw new IllegalArgumentException("Table not defined for factor " + factor.toString());
				config.detail = new MongoDataDetail(s.table(), Factor.NOT_DEFINED.equals(s.filter()) ? null : s.filter());
				if (validate) {
					DataSource<Object, BSONObject, MongoDataDetail> hds = dss.ds(s.source());
					hds.confirm(factor, (MongoDataDetail) config.detail);
				}
				break;
			case CONSTAND_TO_CONSOLE:
				break;
			default:
				throw new UnsupportedOperationException("Unsupportted stocking mode: " + s.type() + " on " + factor.toString());
			}
		}
		return config;
	}
}
