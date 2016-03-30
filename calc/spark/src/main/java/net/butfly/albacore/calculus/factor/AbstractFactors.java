package net.butfly.albacore.calculus.factor;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.bson.BSONObject;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.HbaseDataDetail;
import net.butfly.albacore.calculus.datasource.KafkaDataDetail;
import net.butfly.albacore.calculus.datasource.MongoDataDetail;
import net.butfly.albacore.calculus.factor.Factor.Stocking;
import net.butfly.albacore.calculus.factor.Factor.Streaming;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;

@SuppressWarnings({ "unchecked", "deprecation" })
public abstract class AbstractFactors<D extends Serializable> extends HashMap<String, D> {
	private static final long serialVersionUID = -3712903710207597570L;
	protected Calculator calc;

	protected abstract <K, F extends Factor<F>> JavaPairDStream<K, F> dstream(D rds);

	protected abstract <K, F extends Factor<F>> JavaPairRDD<K, F> rdd(D rds);

	protected abstract <K, F extends Factor<F>> D rds(JavaPairDStream<K, F> dstream);

	protected abstract <K, F extends Factor<F>> D rds(JavaPairRDD<K, F> rdd);

	protected enum DMode {
		RDD(JavaRDDLike.class), DSTREAM(JavaDStreamLike.class);
		protected Class<?> clazz;

		private DMode(Class<?> clazz) {
			this.clazz = clazz;
		}
	}

	protected AbstractFactors(Calculator calc) {
		super(calc.factorings.length);
		this.calc = calc;

		FactorConfig<?, ?> batch = null;
		for (Factoring f : calc.factorings) {
			@SuppressWarnings("rawtypes")
			Class fc = f.factor();
			if (!fc.isAnnotationPresent(Streaming.class) && !fc.isAnnotationPresent(Stocking.class)) throw new IllegalArgumentException(
					"Factor [" + fc.toString() + "] is annotated as neither @Streaming nor @Stocking, can't calculate it!");

			FactorConfig<?, ?> c = config(fc);
			c.batching = f.batching();
			c.streaming = f.stockOnStreaming();
			if (f.batching() > 0) {
				if (batch != null) throw new IllegalArgumentException("Only one batch stocking source supported, now found second: "
						+ batch.factorClass.toString() + " and " + c.factorClass.toString());
				else batch = c;
			}
			read(calc.mode, f.key(), c);
		}
	}

	private <K, F extends Factor<F>> void read(Mode mode, String key, FactorConfig<K, F> config) {
		DataSource<K, ?, ?, DataDetail> ds = calc.dss.ds(config.dbid);
		switch (mode) {
		case STOCKING:
			if (config.batching <= 0)
				add(key, rds(RDDDStream.pstream(calc.ssc, Mechanism.STOCK, () -> ds.stocking(calc, config.factorClass, config.detail))));
			else add(key, rds(ds.batching(calc, config.factorClass, config.batching, config.detail)));
			break;
		case STREAMING:
			switch (config.mode) {
			case STOCKING:
				switch (config.streaming) {
				case CONST:
					put(key, rds(
							RDDDStream.pstream(calc.ssc, Mechanism.CONST, () -> ds.stocking(calc, config.factorClass, config.detail))));
					break;
				case FRESH:
					add(key, rds(
							RDDDStream.pstream(calc.ssc, Mechanism.FRESH, () -> ds.stocking(calc, config.factorClass, config.detail))));
					break;
				default:
					throw new UnsupportedOperationException();
				}
				break;
			case STREAMING:
				add(key, rds(ds.streaming(calc, config.factorClass, config.detail)));
				break;
			}
			break;
		}
	}

	private void add(String key, D value) {
		if (this.containsKey(key)) throw new IllegalArgumentException("Conflictted factoring id: " + key);
		super.put(key, value);
	}

	public <K, F extends Factor<F>> FactorConfig<K, F> config(Class<F> factor) {
		FactorConfig<K, F> config = new FactorConfig<>();
		config.factorClass = factor;
		if (calc.mode == Mode.STREAMING && factor.isAnnotationPresent(Streaming.class)) {
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
				if (calc.validate) {
					DataSource<String, ImmutableBytesWritable, Result, HbaseDataDetail> hds = calc.dss.ds(s.source());
					hds.confirm(factor, (HbaseDataDetail) config.detail);
				}
				break;
			case MONGODB:
				if (Factor.NOT_DEFINED.equals(s.table()))
					throw new IllegalArgumentException("Table not defined for factor " + factor.toString());
				config.detail = new MongoDataDetail(s.table(), Factor.NOT_DEFINED.equals(s.filter()) ? null : s.filter());
				if (calc.validate) {
					DataSource<Object, Object, BSONObject, MongoDataDetail> hds = calc.dss.ds(s.source());
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

	public static AbstractFactors<?> create(Calculator calc) {
		switch (calc.mode) {
		case STOCKING:
			return new PairRDDFactors(calc);
		case STREAMING:
			return new PairDStreamFactors(calc);
		}
		throw new IllegalArgumentException();
	}
}
