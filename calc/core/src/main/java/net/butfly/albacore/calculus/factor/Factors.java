package net.butfly.albacore.calculus.factor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.HbaseDataDetail;
import net.butfly.albacore.calculus.datasource.HiveDataDetail;
import net.butfly.albacore.calculus.datasource.KafkaDataDetail;
import net.butfly.albacore.calculus.datasource.MongoDataDetail;
import net.butfly.albacore.calculus.factor.Directly.StockingDirectly;
import net.butfly.albacore.calculus.factor.Factor.Stocking;
import net.butfly.albacore.calculus.factor.Factor.Streaming;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.Calculating.Calculatings;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import net.butfly.albacore.calculus.utils.Logable;
import net.butfly.albacore.calculus.utils.Reflections;

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
		for (Calculating f : valid(Reflections.multipleAnnotation(Calculating.class, Calculatings.class, calc.calculusClass))) {
			if (CONFIGS.containsKey(f.key())) throw new IllegalArgumentException("Conflictted factoring key: " + f.key());
			@SuppressWarnings("rawtypes")
			Class fc = f.factor();
			if (!fc.isAnnotationPresent(Streaming.class) && !fc.isAnnotationPresent(Stocking.class)) throw new IllegalArgumentException(
					"Factor [" + fc.toString() + "] is annotated as neither @Streaming nor @Stocking, can't calculate it!");
			FactorConfig<?, ?> config = config(fc, f.key());
			cl.add(config.factorClass);
			config.batching = f.batching();
			config.streaming = f.stockOnStreaming();
			config.expanding = f.expanding();
			config.persisting = StorageLevel.fromString(f.persisting());
			if (f.batching() > 0) {
				if (batch != null) throw new IllegalArgumentException("Only one batch stocking source supported, now found second: "
						+ batch.factorClass.toString() + " and " + config.factorClass.toString());
				else batch = config;
			}
			CONFIGS.put(f.key(), config);
		}
		calc.sconf.registerKryoClasses(cl.toArray(new Class[cl.size()]));
	}

	private List<Calculating> valid(List<Calculating> list) {
		Map<String, Calculating> m = new HashMap<>();
		for (Calculating f : list)
			m.putIfAbsent(f.key(), f);
		return new ArrayList<>(m.values());
	}

	public <K, F extends Factor<F>> PairRDS<K, F> directly(String factoringKey, String calcKey) {
		FactorConfig<K, F> config = (FactorConfig<K, F>) CONFIGS.get(factoringKey);
		@SuppressWarnings("rawtypes")
		Map<String, DataDetail> dds = config.directDBIDs.get(factoringKey);
		if (null == dds) return PairRDS.emptyPair(calc.sc);
		DataDetail<F> d = dds.get(calcKey);
		if (null == d) return PairRDS.emptyPair(calc.sc);
		DataSource<K, ?, ?, ?, ?> ds = calc.getDS(d.source);
		if (ds == null) return PairRDS.emptyPair(calc.sc);
		switch (calc.mode) {
		case STOCKING:
			if (config.batching <= 0) return ds.stocking(calc, config.factorClass, d, -1);
			else throw new UnsupportedOperationException();
		case STREAMING:
			switch (config.mode) {
			case STOCKING:
				switch (config.streaming) {
				case CONST:
					return new PairRDS<K, F>(new WrappedDStream<>(
							RDDDStream.pstream(calc.ssc.ssc(), Mechanism.CONST, () -> ds.stocking(calc, config.factorClass, d, -1))));
				case FRESH:
					return new PairRDS<K, F>(new WrappedDStream<>(
							RDDDStream.pstream(calc.ssc.ssc(), Mechanism.FRESH, () -> ds.stocking(calc, config.factorClass, d, -1))));
				default:
					throw new UnsupportedOperationException();
				}
			case STREAMING:
				return new PairRDS<K, F>(new WrappedDStream<>(ds.streaming(calc, config.factorClass, d)));
			}
		default:
			throw new UnsupportedOperationException();
		}
	}

	public <K, F extends Factor<F>> PairRDS<K, F> get(String factorKey, FactorFilter... filters) {
		FactorConfig<K, F> config = (FactorConfig<K, F>) CONFIGS.get(factorKey);
		DataSource<K, ?, ?, ?, ?> ds = calc.getDS(config.dbid);
		if (ds == null) return PairRDS.emptyPair(calc.sc);
		DataDetail<F> d = config.detail;
		switch (calc.mode) {
		case STOCKING:
			if (config.batching <= 0) {
				PairRDS<K, F> p = ds.stocking(calc, config.factorClass, d, config.expanding, filters);
				if (config.persisting != null && !StorageLevel.NONE().equals(config.persisting)) p = p.persist(config.persisting);
				return p;
			} else return new PairRDS<K, F>(new WrappedDStream<>(RDDDStream.bpstream(calc.ssc.ssc(), config.batching,
					(final Long limit, final K offset) -> ds.batching(calc, config.factorClass, limit, offset, d, filters),
					ds.marshaller().comparator())));
		case STREAMING:
			switch (config.mode) {
			case STOCKING:
				switch (config.streaming) {
				case CONST:
					return new PairRDS<K, F>(new WrappedDStream<>(RDDDStream.pstream(calc.ssc.ssc(), Mechanism.CONST,
							() -> ds.stocking(calc, config.factorClass, d, -1, filters))));
				case FRESH:
					return new PairRDS<K, F>(new WrappedDStream<>(RDDDStream.pstream(calc.ssc.ssc(), Mechanism.FRESH,
							() -> ds.stocking(calc, config.factorClass, d, -1, filters))));
				default:
					throw new UnsupportedOperationException();
				}
			case STREAMING:
				// XXX:need repartition?
				return new PairRDS<K, F>(new WrappedDStream<>(ds.streaming(calc, config.factorClass, d, filters)));
			}
		default:
			throw new UnsupportedOperationException();
		}
	}

	public <K, F extends Factor<F>> void put(String key, PairRDS<K, F> rds, Class<K> keyClass, Class<F> factorClass) {
		FactorConfig<K, F> conf = config(factorClass, key);
		DataSource<K, ?, ?, ?, ?> ds = calc.getDS(conf.dbid);
		if (ds != null) rds.save(ds, conf.detail);
	}

	public <K, F extends Factor<F>> FactorConfig<K, F> config(Class<F> factor, String... key) {
		String k = null == key || key.length == 0 ? null : key[0];
		if (null != k && CONFIGS.containsKey(k)) return CONFIGS.get(k);
		if (null == k) // query, but not found
			throw new IllegalArgumentException("Configuration of [" + factor.toString() + "] not found.");
		FactorConfig<K, F> config = new FactorConfig<>();
		config.factorClass = factor;
		config.key = k;

		if (calc.mode == Mode.STREAMING && factor.isAnnotationPresent(Streaming.class)) buildStreaming(config, factor);
		else buildStocking(config, factor);

		buildDirectly(config, factor);
		@SuppressWarnings("rawtypes")
		DataSource mainDS = calc.getDS(config.dbid);
		if (mainDS != null && mainDS.validate) calc.getDS(config.dbid).confirm(factor, config.detail);
		CONFIGS.put(k, config);
		return config;
	}

	private <K, F extends Factor<F>> void buildDirectly(FactorConfig<K, F> config, Class<F> factor) {
		for (StockingDirectly sd : Reflections.multipleAnnotation(StockingDirectly.class, Directly.class, factor)) {
			@SuppressWarnings("rawtypes")
			Map<String, DataDetail> dds;
			if (config.directDBIDs.containsKey(config.key)) dds = config.directDBIDs.get(config.key);
			else {
				dds = new HashMap<>();
				config.directDBIDs.put(config.key, dds);
			}
			DataDetail<F> dd = buildDetail(calc.getDS(sd.source()), factor, sd.type(), sd.source(), null,
					Reflections.construct(sd.query()));
			if (null != dd) dds.putIfAbsent(sd.calc(), dd);
		}
	}

	private <K, F extends Factor<F>> DataDetail<F> buildDetail(@SuppressWarnings("rawtypes") DataSource ds, Class<F> factor, Type type,
			String source, String filter, Supplier<String> query) {
		if (null == ds) return null;
		String q = parseTable(type, factor, source, ds.suffix, query.get());
		switch (type) {
		case HBASE:
			return new HbaseDataDetail<F>(factor, source, q);
		case HIVE:
			return new HiveDataDetail<F>(factor, source, ds.schema, q);
		case MONGODB:
			return new MongoDataDetail<F>(factor, source, null != filter && Factor.NOT_DEFINED.equals(filter) ? null : filter, q);
		case KAFKA:
			return new KafkaDataDetail<F>(factor, source, q);
		case CONSOLE:
			return null;
		default:
			throw new UnsupportedOperationException("Unsupportted stocking mode: " + type + " on " + factor.toString());
		}
	}

	private <K, F extends Factor<F>> void buildStocking(FactorConfig<K, F> config, Class<F> factor) {
		Stocking s = factor.getAnnotation(Stocking.class);
		config.mode = Mode.STOCKING;
		config.dbid = s.source();
		config.detail = buildDetail(calc.getDS(config.dbid), factor, s.type(), s.source(), s.query(), () -> s.table()[0]);
	}

	private <K, F extends Factor<F>> void buildStreaming(FactorConfig<K, F> config, Class<F> factor) {
		config.mode = Mode.STREAMING;
		Streaming s = factor.getAnnotation(Streaming.class);
		config.dbid = s.source();
		config.detail = buildDetail(calc.getDS(config.dbid), factor, s.type(), s.source(), s.query(), () -> s.table()[0]);
	}

	private String parseTable(Type type, Class<? extends Factor<?>> factor, String source, String suffix, String... table) {
		if ((null == table || table.length == 0) && type != Type.CONSOLE)
			throw new IllegalArgumentException("Table not defined for factor " + factor.toString());
		// XXX: now multiple table is not supportted.
		if (null == suffix) return table[0];
		String[] nt = new String[table.length];
		for (int i = 0; i < table.length; i++) {
			nt[i] = table[i] + "_" + suffix;
			final int j = i;
			info(() -> "output redirected on [" + source + "]: [" + table[j] + " => " + nt[j] + "].");
		}
		return nt[0];
	}
}
