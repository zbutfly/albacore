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

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.Calculator.Mode;
import net.butfly.albacore.calculus.datasource.ConsoleDataSource;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.Type;
import net.butfly.albacore.calculus.factor.Calculating.Calculatings;
import net.butfly.albacore.calculus.factor.Factoring.Factorings;
import net.butfly.albacore.calculus.factor.detail.HbaseFractoring;
import net.butfly.albacore.calculus.factor.detail.HiveFractoring;
import net.butfly.albacore.calculus.factor.detail.KafkaFractoring;
import net.butfly.albacore.calculus.factor.detail.MongoFractoring;
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
	@SuppressWarnings("rawtypes")
	private static final Map<String, CalculatingConfig> CONFIGS = new HashMap<>();

	@SuppressWarnings("rawtypes")
	public Factors() {
		CalculatingConfig<?> batchChacking = null;
		Set<Class<?>> kryoClasses = new HashSet<>();
		for (Calculating c : Reflections.multipleAnnotation(Calculating.class, Calculatings.class, Calculator.calculator.calculusClass)) {
			if (CONFIGS.containsKey(c.key())) throw new IllegalArgumentException("Conflictted factoring key: " + c.key());
			Class<? extends Factor> fc = (Class<? extends Factor>) c.factor();
			CalculatingConfig config = buildConfig(fc, scanFractoring(Calculator.calculator.mode, Reflections.multipleAnnotation(
					Factoring.class, Factorings.class, fc)));
			if (null != config) {
				config.key = c.key();
				config.batching = c.batching();
				config.streaming = c.stockOnStreaming();
				config.expanding = c.expanding();
				config.persisting = c.persisting().level();
				config.factorClass = fc;
				if (c.batching() > 0) {
					if (batchChacking != null) throw new IllegalArgumentException(
							"Only one batch stocking source supported, now found second: " + batchChacking.factorClass.toString() + " and "
									+ config.factorClass.toString());
					else batchChacking = config;
				}
				kryoClasses.add(fc);
				CONFIGS.putIfAbsent(c.key(), config);
			}
		}
		Calculator.calculator.sconf.registerKryoClasses(kryoClasses.toArray(new Class[kryoClasses.size()]));
		for (Factoring f : Reflections.multipleAnnotation(Factoring.class, Factorings.class, Calculator.calculator.calculusClass)) {
			CalculatingConfig c = CONFIGS.get(f.key().split("\\.")[0]);
			if (null == c) throw new IllegalArgumentException("Parent @Calculating not found for @Factoring " + f.key());
			CalculatingConfig config = buildConfig(c.factorClass, f);
			config.key = f.key();
			config.batching = c.batching;
			config.streaming = c.streaming;
			config.expanding = c.expanding;
			config.persisting = c.persisting;
			config.factorClass = c.factorClass;
			CONFIGS.put(f.key(), config);
		}
	}

	private <F extends Factor<F>> CalculatingConfig<F> buildConfig(Class<F> factor, Factoring f) {
		@SuppressWarnings("rawtypes")
		DataSource ds = Type.CONSOLE.name().equals(f.ds()) ? ConsoleDataSource.CONSOLE_DS : Calculator.calculator.getDS(f.ds());
		if (null == ds) return null; // ignore datasource not defined.
		CalculatingConfig<F> cfg = new CalculatingConfig<>();
		cfg.mode = f.mode();
		cfg.dbid = f.ds();
		String table = parseTable(factor, f, Calculator.calculator.getDS(f.ds()).suffix);
		String query = parseQuery(factor, f);
		switch (ds.type()) {
		case HBASE:
			cfg.factoring = new HbaseFractoring<F>(factor, f.ds(), table);
			break;
		case HIVE:
			cfg.factoring = new HiveFractoring<F>(factor, f.ds(), table, query);
			break;
		case MONGODB:
			cfg.factoring = new MongoFractoring<F>(factor, f.ds(), table, query);
			break;
		case KAFKA:
			cfg.factoring = new KafkaFractoring<F>(factor, f.ds(), table, query);
			break;
		case CONSOLE:
			cfg.factoring = null;
			break;
		default:
			throw new UnsupportedOperationException("Unsupportted mode: " + ds.type() + " on " + factor.toString());
		}
		if (ds.validate) ds.confirm(factor, cfg.factoring);
		return cfg;
	}

	public <K, F extends Factor<F>> PairRDS<K, F> get(String key, FactorFilter... filters) {
		CalculatingConfig<F> config = (CalculatingConfig<F>) CONFIGS.get(key);
		if (config == null) throw new IllegalArgumentException("Config " + key + " not found!");
		DataSource<K, ?, ?, ?, ?> ds = Calculator.calculator.getDS(config.dbid);
		if (ds == null) return PairRDS.emptyPair(Calculator.calculator.sc);
		FactroingConfig<F> d = config.factoring;
		switch (Calculator.calculator.mode) {
		case STOCKING:
			if (config.batching <= 0) {
				PairRDS<K, F> p = ds.stocking(config.factorClass, d, config.expanding, filters);
				if (config.persisting != null && !StorageLevel.NONE().equals(config.persisting)) p = p.persist(config.persisting);
				return p;
			} else return new PairRDS<K, F>(new WrappedDStream<>(RDDDStream.bpstream(Calculator.calculator.ssc.ssc(), config.batching, (
					final Long limit, final K offset) -> ds.batching(config.factorClass, limit, offset, d, filters), ds.marshaller()
							.comparator())));
		case STREAMING:
			switch (config.mode) {
			case STOCKING:
				switch (config.streaming) {
				case CONST:
					return new PairRDS<K, F>(new WrappedDStream<>(RDDDStream.pstream(Calculator.calculator.ssc.ssc(), Mechanism.CONST,
							() -> ds.stocking(config.factorClass, d, -1, filters))));
				case FRESH:
					return new PairRDS<K, F>(new WrappedDStream<>(RDDDStream.pstream(Calculator.calculator.ssc.ssc(), Mechanism.FRESH,
							() -> ds.stocking(config.factorClass, d, -1, filters))));
				default:
					throw new UnsupportedOperationException();
				}
			case STREAMING:
				// XXX:need repartition?
				return new PairRDS<K, F>(new WrappedDStream<>(ds.streaming(config.factorClass, d, filters)));
			}
		default:
			throw new UnsupportedOperationException();
		}
	}

	public <K, F extends Factor<F>> void put(String key, PairRDS<K, F> rds, Class<K> keyClass, Class<F> factorClass) {
		CalculatingConfig<F> conf = CONFIGS.get(key);
		if (null == conf) {
			warn(() -> key + " not defined as output for " + factorClass.toString() + " .");
			return;
		}
		DataSource<K, ?, ?, ?, ?> ds = Calculator.calculator.getDS(conf.dbid);
		if (ds == null) {
			warn(() -> conf.dbid + " not defined on outputing to it for " + factorClass.toString() + " .");
			return;
		}
		rds.save(ds, conf.factoring);
	}

	private String parseTable(Class<? extends Factor<?>> factor, Factoring s, String suffix) {
		if (null == s.table() || s.table().length == 0) return null;
		info(() -> "Multiple tables defining in @Factoring is not supportted (currently).");
		if (null == suffix) return s.table()[0];
		String[] nt = new String[s.table().length];
		for (int i = 0; i < s.table().length; i++) {
			nt[i] = s.table()[i] + "_" + suffix;
			final int j = i;
			info(() -> "output redirected on [" + s.ds() + "]: [" + s.table()[j] + " => " + nt[j] + "].");
		}
		return nt[0];
	}

	private String parseQuery(Class<? extends Factor<?>> factor, Factoring s) {
		if (s.query().length == 0 && s.querying().length == 0) return null;
		if (s.query().length > 0 && s.querying().length > 0) throw new IllegalArgumentException("Define either query or querying on " + s
				.ds() + " at " + factor.toString() + ", both not supportted.");
		if (s.query().length == 1) return s.query()[0];
		if (s.querying().length == 1) return Reflections.construct(s.querying()[0]).get();
		if (s.query().length == 0) {
			List<String> qs = new ArrayList<>();
			for (Class<? extends Supplier<String>> sp : s.querying())
				qs.add(Reflections.construct(sp).get());
			return Calculator.calculator.getDS(s.ds()).andQuery(qs.toArray(new String[qs.size()]));
		}
		if (s.querying().length == 0) return Calculator.calculator.getDS(s.ds()).andQuery(s.query());
		throw new IllegalArgumentException("Why here?!");
	}

	private Factoring scanFractoring(Mode mode, List<Factoring> factorings) {
		for (Factoring f : factorings)
			if (f.mode() == mode) return f;
		if (mode == Mode.STREAMING) for (Factoring f : factorings)
			if (f.mode() == Mode.STOCKING) return f;
		throw new IllegalArgumentException("Correct @Factoring for mode " + Calculator.calculator.mode + " not found.");
	}
}