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
import net.butfly.albacore.calculus.Calculator.Mode;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.factor.Calculating.Calculatings;
import net.butfly.albacore.calculus.factor.Factoring.Factorings;
import net.butfly.albacore.calculus.factor.Factoring.Type;
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
	protected static final Logger logger = LoggerFactory.getLogger(Factors.class);
	public Calculator calc;
	@SuppressWarnings("rawtypes")
	private static final Map<String, CalculatingConfig> CONFIGS = new HashMap<>();

	@SuppressWarnings("rawtypes")
	public Factors(Calculator calc) {
		this.calc = calc;
		CalculatingConfig<?> batchChacking = null;
		Set<Class<?>> kryoClasses = new HashSet<>();
		for (Calculating c : Reflections.multipleAnnotation(Calculating.class, Calculatings.class, calc.calculusClass)) {
			if (CONFIGS.containsKey(c.key())) throw new IllegalArgumentException("Conflictted factoring key: " + c.key());
			Class<? extends Factor> fc = (Class<? extends Factor>) c.factor();
			CalculatingConfig config = buildConfig(fc, scanFractoring(calc.mode, Reflections.multipleAnnotation(Factoring.class,
					Factorings.class, fc)));
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
		calc.sconf.registerKryoClasses(kryoClasses.toArray(new Class[kryoClasses.size()]));
		for (Factoring f : Reflections.multipleAnnotation(Factoring.class, Factorings.class, calc.calculusClass)) {
			CalculatingConfig c = CONFIGS.get(f.key().split(".")[0]);
			if (null == c) throw new IllegalArgumentException("Parent @Calculating not found for @Factoring " + f.key());
			CalculatingConfig config = buildConfig(c.factorClass, f);
			config.key = f.key();
			config.batching = c.batching;
			config.streaming = c.streaming;
			config.expanding = c.expanding;
			config.persisting = c.persisting;
			CONFIGS.put(f.key(), config);
		}
	}

	private <F extends Factor<F>> CalculatingConfig<F> buildConfig(Class<F> factor, Factoring f) {
		// ignore datasource not defined.
		@SuppressWarnings("rawtypes")
		DataSource ds = calc.getDS(f.ds());
		if (null == ds) return null;
		CalculatingConfig<F> cfg = new CalculatingConfig<>();
		cfg.mode = f.mode();
		cfg.dbid = f.ds();
		String table = parseTable(factor, f, calc.getDS(f.ds()).suffix);
		String query = parseQuery(factor, f);

		switch (f.type()) {
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
			throw new UnsupportedOperationException("Unsupportted stocking mode: " + f.type() + " on " + factor.toString());
		}
		if (ds.validate) ds.confirm(factor, cfg.factoring);
		return cfg;
	}

	public <K, F extends Factor<F>> PairRDS<K, F> get(String key, FactorFilter... filters) {
		CalculatingConfig<F> config = (CalculatingConfig<F>) CONFIGS.get(key);
		DataSource<K, ?, ?, ?, ?> ds = calc.getDS(config.dbid);
		if (ds == null) return PairRDS.emptyPair(calc.sc);
		FactroingConfig<F> d = config.factoring;
		switch (calc.mode) {
		case STOCKING:
			if (config.batching <= 0) {
				PairRDS<K, F> p = ds.stocking(calc, config.factorClass, d, config.expanding, filters);
				if (config.persisting != null && !StorageLevel.NONE().equals(config.persisting)) p = p.persist(config.persisting);
				return p;
			} else return new PairRDS<K, F>(new WrappedDStream<>(RDDDStream.bpstream(calc.ssc.ssc(), config.batching, (final Long limit,
					final K offset) -> ds.batching(calc, config.factorClass, limit, offset, d, filters), ds.marshaller().comparator())));
		case STREAMING:
			switch (config.mode) {
			case STOCKING:
				switch (config.streaming) {
				case CONST:
					return new PairRDS<K, F>(new WrappedDStream<>(RDDDStream.pstream(calc.ssc.ssc(), Mechanism.CONST, () -> ds.stocking(
							calc, config.factorClass, d, -1, filters))));
				case FRESH:
					return new PairRDS<K, F>(new WrappedDStream<>(RDDDStream.pstream(calc.ssc.ssc(), Mechanism.FRESH, () -> ds.stocking(
							calc, config.factorClass, d, -1, filters))));
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
		CalculatingConfig<F> conf = CONFIGS.get(key);
		if (null == conf) {
			warn(() -> key + " not defined as output for " + factorClass.toString() + " .");
			return;
		}
		DataSource<K, ?, ?, ?, ?> ds = calc.getDS(conf.dbid);
		if (ds == null) {
			warn(() -> conf.dbid + " not defined on outputing to it for " + factorClass.toString() + " .");
			return;
		}
		rds.save(ds, conf.factoring);
	}

	private String parseTable(Class<? extends Factor<?>> factor, Factoring s, String suffix) {
		if ((null == s.table() || s.table().length == 0) && s.type() != Type.CONSOLE) throw new IllegalArgumentException(
				"Table not defined for factor " + factor.toString());
		if (s.type() == Type.CONSOLE) return null;
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
			return calc.getDS(s.ds()).andQuery(qs.toArray(new String[qs.size()]));
		}
		if (s.querying().length == 0) return calc.getDS(s.ds()).andQuery(s.query());
		throw new IllegalArgumentException("Why here?!");
	}

	private Factoring scanFractoring(Mode mode, List<Factoring> factorings) {
		for (Factoring f : factorings)
			if (f.mode() == mode) return f;
		if (mode == Mode.STREAMING) for (Factoring f : factorings)
			if (f.mode() == Mode.STOCKING) return f;
		throw new IllegalArgumentException("Correct @Factoring for mode " + calc.mode + " not found.");
	}
}