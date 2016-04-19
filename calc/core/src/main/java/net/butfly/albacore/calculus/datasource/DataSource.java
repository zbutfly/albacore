package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.utils.Logable;
import scala.Tuple2;

public abstract class DataSource<FK, RK, RV, WK, WV> implements Serializable, Logable {
	private static final long serialVersionUID = 1L;
	protected Factor.Type type;
	protected Marshaller<FK, RK, RV> marshaller;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	public boolean validate;
	public String suffix;
	public Class<RK> keyClass;
	public Class<RV> valueClass;
	@SuppressWarnings("rawtypes")
	public Class<? extends OutputFormat> outputFormatClass;
	protected Configuration outputConfig;

	public Factor.Type type() {
		return type;
	}

	public Marshaller<FK, RK, RV> marshaller() {
		return marshaller;
	}

	public DataSource(Type type, boolean validate, Marshaller<FK, RK, RV> marshaller, Class<RK> keyClass, Class<RV> valueClass,
			@SuppressWarnings("rawtypes") Class<? extends OutputFormat> outputFormatClass) {
		super();
		this.type = type;
		this.validate = validate;
		this.marshaller = marshaller;
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.outputFormatClass = outputFormatClass;
	}

	@Override
	public String toString() {
		return "CalculatorDataSource:" + this.type;
	}

	public <F extends Factor<F>> JavaPairRDD<FK, F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail,
			FactorFilter... filters) {
		throw new UnsupportedOperationException("Unsupportted stocking mode: " + type + " on " + factor.toString());
	}

	@Deprecated
	public <F extends Factor<F>> JavaPairRDD<FK, F> batching(Calculator calc, Class<F> factorClass, long batching, FK offset,
			DataDetail<F> detail, FactorFilter... filters) {
		throw new UnsupportedOperationException("Unsupportted stocking mode with batching: " + type + " on " + factorClass.toString());
	}

	public <F extends Factor<F>> JavaPairDStream<FK, F> streaming(Calculator calc, Class<F> factor, DataDetail<F> detail,
			FactorFilter... filters) {
		throw new UnsupportedOperationException("Unsupportted streaming mode: " + type + " on " + factor.toString());
	}

	public <F> boolean confirm(Class<F> factor, DataDetail<F> detail) {
		return true;
	}

	public static class DataSources extends HashMap<String, DataSource<?, ?, ?, ?, ?>> {
		private static final long serialVersionUID = -7809799411800022817L;

		@SuppressWarnings("unchecked")
		public <FK, RK, RV, WK, WV, DS extends DataSource<FK, RK, RV, WK, WV>> DS ds(String dbid) {
			return (DS) super.get(dbid);
		}
	}

	public <F> Tuple2<WK, WV> prepare(FK key, F factor, Class<F> factorClass) throws Exception {
		throw new UnsupportedOperationException("Unsupportted saving prepare: " + type);
	}
}
