package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.Filter;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.lambda.VoidFunction;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.utils.Logable;

public abstract class DataSource<FK, K, V, D extends DataDetail> implements Serializable, Logable {
	private static final long serialVersionUID = 1L;
	protected Factor.Type type;
	protected Marshaller<FK, K, V> marshaller;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	public boolean validate;
	public String suffix;

	public Factor.Type type() {
		return type;
	}

	public Marshaller<FK, K, V> marshaller() {
		return marshaller;
	}

	public DataSource(Type type, boolean validate, Marshaller<FK, K, V> marshaller) {
		super();
		this.type = type;
		this.validate = validate;
		this.marshaller = marshaller;
	}

	@Override
	public String toString() {
		return "CalculatorDataSource:" + this.type;
	}

	public <F extends Factor<F>> JavaPairRDD<FK, F> stocking(Calculator calc, Class<F> factor, D detail, Filter... filters) {
		throw new UnsupportedOperationException("Unsupportted stocking mode: " + type + " on " + factor.toString());
	}

	@Deprecated
	public <F extends Factor<F>> JavaPairRDD<FK, F> batching(Calculator calc, Class<F> factorClass, long batching, FK offset, D detail,
			Filter... filters) {
		throw new UnsupportedOperationException("Unsupportted stocking mode with batching: " + type + " on " + factorClass.toString());
	}

	public <F extends Factor<F>> JavaPairDStream<FK, F> streaming(Calculator calc, Class<F> factor, D detail, Filter... filters) {
		throw new UnsupportedOperationException("Unsupportted streaming mode: " + type + " on " + factor.toString());
	}

	public <F extends Factor<F>> VoidFunction<JavaPairRDD<FK, F>> saving(Calculator calc, D detail) {
		throw new UnsupportedOperationException("Unsupportted saving: " + type);
	}

	public boolean confirm(Class<? extends Factor<?>> factor, D detail) {
		return true;
	}

	public <F extends Factor<F>> void save(Calculator calc, PairRDS<FK, F> result, D detail) {
		VoidFunction<JavaPairRDD<FK, F>> saving = saving(calc, detail);
		if (null != result) result.eachPairRDD((VoidFunction<JavaPairRDD<FK, F>>) rdd -> {
			if (null != rdd) try {
				saving.call(rdd);
			} catch (Exception e) {
				error(() -> "Saving failure", e);
			}
		});
	}

	public static class DataSources extends HashMap<String, DataSource<?, ?, ?, ?>> {
		private static final long serialVersionUID = -7809799411800022817L;

		@SuppressWarnings("unchecked")
		public <FK, K, V, D extends DataDetail> DataSource<FK, K, V, D> ds(String dbid) {
			return (DataSource<FK, K, V, D>) super.get(dbid);
		}
	}
}
