package net.butfly.albacore.calculus.factor;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger logger = LoggerFactory.getLogger(Factors.class);
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

	public <K, F extends Factor<F>, E extends Factor<E>> PairRDS<K, F> get(String factoring, String field, Collection<?> other) {
		FactorConfig<K, F> config = (FactorConfig<K, F>) pool.get(factoring);
		DataSource<K, ?, ?, DataDetail> ds = calc.dss.ds(config.dbid);
		switch (calc.mode) {
		case STOCKING:
			if (config.batching <= 0) return new PairRDS<K, F>(ds.stocking(calc, config.factorClass, config.detail, field, other));
			else return new PairRDS<K, F>(RDDDStream.bpstream(calc.ssc.ssc(), config.batching,
					(limit, offset) -> ds.batching(calc, config.factorClass, limit, offset, config.detail), ds.marshaller().comparator()));
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

	public <K, F extends Factor<F>> PairRDS<K, F> get(String factoring, String field, PairRDS<?, ?> other) {
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
				if (null == s.table() || s.table().length == 0)
					throw new IllegalArgumentException("Table not defined for factor " + factor.toString());
				config.detail = new HbaseDataDetail(s.table());
				config.keyClass = (Class<K>) byte[].class;
				break;
			case MONGODB:
				if (null == s.table() || s.table().length == 0)
					throw new IllegalArgumentException("Table not defined for factor " + factor.toString());
				String suffix = calc.dss.ds(config.dbid).suffix;
				if (null != suffix) {
					String[] nt = new String[s.table().length];
					logger.info("All output mongodb table on " + s.source() + " will be appendded suffix: " + suffix);
					for (int i = 0; i < s.table().length; i++)
						nt[i] = s.table()[i] + "_" + suffix;
					config.detail = new MongoDataDetail(Factor.NOT_DEFINED.equals(s.filter()) ? null : s.filter(), nt);
				} else config.detail = new MongoDataDetail(Factor.NOT_DEFINED.equals(s.filter()) ? null : s.filter(), s.table());
				config.keyClass = (Class<K>) Object.class;
				break;
			case CONSTAND_TO_CONSOLE:
				break;
			default:
				throw new UnsupportedOperationException("Unsupportted stocking mode: " + s.type() + " on " + factor.toString());
			}
		}
		if (calc.dss.ds(config.dbid).validate) calc.dss.ds(config.dbid).confirm(factor, config.detail);
		return config;
	}
}
