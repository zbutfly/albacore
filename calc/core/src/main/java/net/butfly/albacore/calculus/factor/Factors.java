package net.butfly.albacore.calculus.factor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;

@SuppressWarnings({ "unchecked", "deprecation" })
public final class Factors implements Serializable {
	private static final long serialVersionUID = -3712903710207597570L;
	protected Calculator calc;
	protected Map<String, FactorConfig<?, ?>> pool;

	public Factors(Calculator calc) {
		pool = new HashMap<>(calc.factorings.length);
		this.calc = calc;

		FactorConfig<?, ?> batch = null;
		for (Factoring f : calc.factorings) {
			if (pool.containsKey(f.key())) throw new IllegalArgumentException("Conflictted factoring id: " + f.key());
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
			pool.put(f.key(), c);
		}
	}

	public <K, F extends Factor<F>> PairRDS<K, F> get(String factoring) {
		return get(factoring, null, new HashSet<>());
	}

	public <K, F extends Factor<F>, E extends Factor<E>> PairRDS<K, F> get(String factoring, String field, Set<?> other) {
		FactorConfig<K, F> config = (FactorConfig<K, F>) pool.get(factoring);
		DataSource<K, ?, ?, DataDetail> ds = calc.dss.ds(config.dbid);
		switch (calc.mode) {
		case STOCKING:
			if (config.batching <= 0) return new PairRDS<K, F>(ds.stocking(calc, config.factorClass, config.detail, field, other));
			else return new PairRDS<K, F>(RDDDStream.bpstream(calc.ssc.ssc(), config.batching,
					(limit, offset) -> ds.batching(calc, config.factorClass, limit, offset, config.detail), ds.marshaller().comparator()));
			// batching, no foreign key refer.
		case STREAMING:
			switch (config.mode) {
			case STOCKING:
				switch (config.streaming) {
				case CONST:
					return new PairRDS<K, F>(RDDDStream.pstream(calc.ssc.ssc(), Mechanism.CONST,
							() -> ds.stocking(calc, config.factorClass, config.detail, field, other)));
				case FRESH:
					return new PairRDS<K, F>(RDDDStream.pstream(calc.ssc.ssc(), Mechanism.FRESH,
							() -> ds.stocking(calc, config.factorClass, config.detail, field, other)));
				default:
					throw new UnsupportedOperationException();
				}
			case STREAMING:
				return new PairRDS<K, F>(ds.streaming(calc, config.factorClass, config.detail));
			}
		default:
			throw new UnsupportedOperationException();
		}
	}

	public <K, F extends Factor<F>> PairRDS<K, F> get(String factoring, String field, PairRDS<K, ?> other) {
		return get(factoring, field, other.collectKeys());
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
				config.keyClass = (Class<K>) String.class;
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
				config.keyClass = (Class<K>) byte[].class;
				break;
			case MONGODB:
				if (Factor.NOT_DEFINED.equals(s.table()))
					throw new IllegalArgumentException("Table not defined for factor " + factor.toString());
				config.detail = new MongoDataDetail(s.table(), Factor.NOT_DEFINED.equals(s.filter()) ? null : s.filter());
				if (calc.validate) {
					DataSource<Object, Object, BSONObject, MongoDataDetail> hds = calc.dss.ds(s.source());
					hds.confirm(factor, (MongoDataDetail) config.detail);
				}
				config.keyClass = (Class<K>) Object.class;
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
