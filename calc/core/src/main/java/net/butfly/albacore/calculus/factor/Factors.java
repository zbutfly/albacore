package net.butfly.albacore.calculus.factor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
	protected Map<String, FactorConfig<?, ?>> pool;

	public Factors(Calculator calc) {
		Factoring[] factorings;
		if (calc.calculusClass.isAnnotationPresent(Factorings.class))
			factorings = calc.calculusClass.getAnnotation(Factorings.class).value();
		else if (calc.calculusClass.isAnnotationPresent(Factoring.class))
			factorings = new Factoring[] { calc.calculusClass.getAnnotation(Factoring.class) };
		else throw new IllegalArgumentException("Calculus " + calc.calculusClass.toString() + " has no @Factoring annotated.");
		pool = new HashMap<>(factorings.length);
		this.calc = calc;

		FactorConfig<?, ?> batch = null;
		List<Class<?>> cl = new ArrayList<>();
		for (Factoring f : factorings) {
			if (pool.containsKey(f.key())) throw new IllegalArgumentException("Conflictted factoring id: " + f.key());
			@SuppressWarnings("rawtypes")
			Class fc = f.factor();
			if (!fc.isAnnotationPresent(Streaming.class) && !fc.isAnnotationPresent(Stocking.class)) throw new IllegalArgumentException(
					"Factor [" + fc.toString() + "] is annotated as neither @Streaming nor @Stocking, can't calculate it!");
			FactorConfig<?, ?> conf = config(fc);
			cl.add(conf.factorClass);
			conf.batching = f.batching();
			conf.streaming = f.stockOnStreaming();
			if (f.batching() > 0) {
				if (batch != null) throw new IllegalArgumentException("Only one batch stocking source supported, now found second: "
						+ batch.factorClass.toString() + " and " + conf.factorClass.toString());
				else batch = conf;
			}
			pool.put(f.key(), conf);
		}
		calc.sconf.registerKryoClasses(cl.toArray(new Class[cl.size()]));
	}

	public <K, F extends Factor<F>, E extends Factor<E>> PairRDS<K, F> get(String factoring, FactorFilter... filters) {
		FactorConfig<K, F> config = (FactorConfig<K, F>) pool.get(factoring);
		DataSource<K, ?, ?, ?, ?> ds = calc.dss.ds(config.dbid);
		DataDetail<F> d = config.detail;
		switch (calc.mode) {
		case STOCKING:
			if (config.batching <= 0) return new PairRDS<K, F>(ds.stocking(calc, config.factorClass, d, filters));
			else return new PairRDS<K, F>(RDDDStream.bpstream(calc.ssc.ssc(), config.batching,
					(final Long limit, final K offset) -> ds.batching(calc, config.factorClass, limit, offset, d, filters),
					ds.marshaller().comparator()));
		case STREAMING:
			switch (config.mode) {
			case STOCKING:
				switch (config.streaming) {
				case CONST:
					return new PairRDS<K, F>(
							RDDDStream.pstream(calc.ssc.ssc(), Mechanism.CONST, () -> ds.stocking(calc, config.factorClass, d, filters)));
				case FRESH:
					return new PairRDS<K, F>(
							RDDDStream.pstream(calc.ssc.ssc(), Mechanism.FRESH, () -> ds.stocking(calc, config.factorClass, d, filters)));
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

	public <K, F extends Factor<F>> FactorConfig<K, F> config(Class<F> factor) {
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
		return config;
	}
}
