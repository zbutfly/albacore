package net.butfly.albacore.calculus.factor;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.HbaseDataDetail;
import net.butfly.albacore.calculus.datasource.KafkaDataDetail;
import net.butfly.albacore.calculus.datasource.MongoDataDetail;
import net.butfly.albacore.calculus.datasource.MongoDataSource;
import net.butfly.albacore.calculus.factor.Factor.Stocking;
import net.butfly.albacore.calculus.factor.Factor.Streaming;
import net.butfly.albacore.calculus.factor.Factoring.Factorings;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import net.butfly.albacore.calculus.utils.Logable;

@SuppressWarnings({ "unchecked", "deprecation" })
public final class Factors implements Serializable, Logable {
	private static final long serialVersionUID = -3712903710207597570L;
	protected static final Logger logger = LoggerFactory.getLogger(Factors.class);
	public Calculator calc;
	@SuppressWarnings("rawtypes")
	private static final Map<String, FactorConfig> CONFIGS = new HashMap<>();
	// private Map<String, FactorConfig<?, ?>> pool;

	public Factors(Calculator calc) {
		this.calc = calc;
		FactorConfig<?, ?> batch = null;
		Set<Class<?>> cl = new HashSet<>();
		for (Factoring fing : scanFactorings(calc.calculusClass)) {
			if (CONFIGS.containsKey(fing.key())) throw new IllegalArgumentException("Conflictted factoring id: " + fing.key());
			@SuppressWarnings("rawtypes")
			Class fc = fing.factor();
			if (!fc.isAnnotationPresent(Streaming.class) && !fc.isAnnotationPresent(Stocking.class)) throw new IllegalArgumentException(
					"Factor [" + fc.toString() + "] is annotated as neither @Streaming nor @Stocking, can't calculate it!");
			FactorConfig<?, ?> conf = config(fc);
			cl.add(conf.factorClass);
			conf.batching = fing.batching();
			conf.streaming = fing.stockOnStreaming();
			conf.expanding = fing.expanding();
			conf.persisting = StorageLevel.fromString(fing.persisting());
			if (fing.batching() > 0) {
				if (batch != null) throw new IllegalArgumentException("Only one batch stocking source supported, now found second: "
						+ batch.factorClass.toString() + " and " + conf.factorClass.toString());
				else batch = conf;
			}
			CONFIGS.put(fing.key(), conf);
		}
		calc.sconf.registerKryoClasses(cl.toArray(new Class[cl.size()]));
	}

	private Collection<Factoring> scanFactorings(Class<?> c) {
		Map<String, Factoring> fs = new HashMap<>();
		if (null != c) {

			if (c.isAnnotationPresent(Factorings.class)) {
				for (Factoring f : c.getAnnotation(Factorings.class).value())
					fs.putIfAbsent(f.key(), f);
			} else if (c.isAnnotationPresent(Factoring.class)) {
				Factoring f = c.getAnnotation(Factoring.class);
				if (!fs.containsKey(f.key())) fs.put(f.key(), f);
			}
			for (Class<?> ci : c.getInterfaces())
				for (Factoring f : scanFactorings(ci))
					fs.putIfAbsent(f.key(), f);
			for (Factoring f : scanFactorings(c.getSuperclass()))
				fs.putIfAbsent(f.key(), f);
		}
		return fs.values();
	}

	public <K, F extends Factor<F>> PairRDS<K, F> get(String factoring, FactorFilter... filters) {
		FactorConfig<K, F> config = (FactorConfig<K, F>) CONFIGS.get(factoring);
		DataSource<K, ?, ?, ?, ?> ds = calc.dss.ds(config.dbid);
		DataDetail<F> d = config.detail;
		switch (calc.mode) {
		case STOCKING:
			if (config.batching <= 0) {
				PairRDS<K, F> p = new PairRDS<K, F>(ds.stocking(calc, config.factorClass, d, config.expanding, filters));
				if (config.persisting != null) p = p.persist(config.persisting);
				return p;
			} else return new PairRDS<K, F>(RDDDStream.bpstream(calc.ssc.ssc(), config.batching,
					(final Long limit, final K offset) -> ds.batching(calc, config.factorClass, limit, offset, d, filters),
					ds.marshaller().comparator()));
		case STREAMING:
			switch (config.mode) {
			case STOCKING:
				switch (config.streaming) {
				case CONST:
					return new PairRDS<K, F>(RDDDStream.pstream(calc.ssc.ssc(), Mechanism.CONST,
							() -> ds.stocking(calc, config.factorClass, d, -1, filters)));
				case FRESH:
					return new PairRDS<K, F>(RDDDStream.pstream(calc.ssc.ssc(), Mechanism.FRESH,
							() -> ds.stocking(calc, config.factorClass, d, -1, filters)));
				default:
					throw new UnsupportedOperationException();
				}
			case STREAMING:
				return new PairRDS<K, F>(ds.streaming(calc, config.factorClass, d, filters));
			}
		default:
			throw new UnsupportedOperationException();
		}
	}

	public <K, F extends Factor<F>> void put(PairRDS<K, F> rds, Class<K> keyClass, Class<F> factorClass) {
		FactorConfig<K, F> conf = config(factorClass);
		DataSource<K, ?, ?, ?, ?> ds = calc.dss.ds(conf.dbid);
		rds.save(ds, conf.detail);
	}

	public <K, F extends Factor<F>> FactorConfig<K, F> config(Class<F> factor) {
		if (CONFIGS.containsKey(factor)) return CONFIGS.get(factor);
		FactorConfig<K, F> config = new FactorConfig<>();
		config.factorClass = factor;
		if (calc.mode == Mode.STREAMING && factor.isAnnotationPresent(Streaming.class)) {
			config.mode = Mode.STREAMING;
			Streaming s = factor.getAnnotation(Streaming.class);
			config.dbid = s.source();
			switch (s.type()) {
			case KAFKA:
				config.detail = new KafkaDataDetail<F>(factor, s.table());
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
				config.detail = new HbaseDataDetail<F>(factor, s.table());
				config.keyClass = (Class<K>) byte[].class;
				break;
			case MONGODB:
				if (null == s.table() || s.table().length == 0)
					throw new IllegalArgumentException("Table not defined for factor " + factor.toString());
				MongoDataSource ds = calc.dss.ds(config.dbid);
				String suffix = ds.suffix;
				if (null != suffix) {
					String[] nt = new String[s.table().length];
					for (int i = 0; i < s.table().length; i++) {
						nt[i] = s.table()[i] + "_" + suffix;
						final int j = i;
						info(() -> "output redirected on [" + s.source() + "]: [" + s.table()[j] + " => " + nt[j] + "].");
					}
					config.detail = new MongoDataDetail<F>(factor, Factor.NOT_DEFINED.equals(s.filter()) ? null : s.filter(), nt);
				} else config.detail = new MongoDataDetail<F>(factor, Factor.NOT_DEFINED.equals(s.filter()) ? null : s.filter(), s.table());
				config.keyClass = (Class<K>) Object.class;
				break;
			case CONSTAND_TO_CONSOLE:
				break;
			default:
				throw new UnsupportedOperationException("Unsupportted stocking mode: " + s.type() + " on " + factor.toString());
			}
		}
		if (calc.dss.ds(config.dbid).validate) calc.dss.ds(config.dbid).confirm(factor, config.detail);
		CONFIGS.put(factor.toString(), config);
		return config;
	}
}
