package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.marshall.Marshaller;

public abstract class DataSource<K, V, D extends DataDetail> implements Serializable {
	private static final long serialVersionUID = 1L;
	Factor.Type type;
	Marshaller<K, V> marshaller;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	public Factor.Type getType() {
		return type;
	}

	public Marshaller<K, V> getMarshaller() {
		return marshaller;
	}

	public DataSource(Type type, Marshaller<K, V> marshaller) {
		super();
		this.type = type;
		this.marshaller = marshaller;
	}

	@Override
	public String toString() {
		return "CalculatorDataSource:" + this.type;
	}

	public <KK, F extends Factor<F>> JavaPairRDD<KK, F> stocking(JavaSparkContext sc, Class<F> factor, D detail) {
		throw new UnsupportedOperationException("Unsupportted stocking mode: " + type + " on " + factor.toString());
	}

	public <KK, F extends Factor<F>> JavaPairInputDStream<KK, F> batching(JavaStreamingContext ssc, Class<F> factor, long batching,
			D detail, Class<KK> kClass, Class<F> vClass) {
		throw new UnsupportedOperationException("Unsupportted stocking mode with batching: " + type + " on " + factor.toString());
	}

	public <KK, F extends Factor<F>> JavaPairInputDStream<KK, F> streaming(JavaStreamingContext ssc, Class<F> factor, D detail) {
		throw new UnsupportedOperationException("Unsupportted streaming mode: " + type + " on " + factor.toString());
	}

	public <KK, F extends Factor<F>> VoidFunction<JavaPairRDD<KK, F>> saving(JavaSparkContext sc, D detail) {
		throw new UnsupportedOperationException("Unsupportted saving: " + type);
	}

	public boolean confirm(Class<? extends Factor<?>> factor, D detail) {
		return true;
	}

	public static class DataSources extends HashMap<String, DataSource<?, ?, ?>> {
		private static final long serialVersionUID = -7809799411800022817L;

		@SuppressWarnings("unchecked")
		public <K, V, D extends DataDetail> DataSource<K, V, D> ds(String dbid) {
			return (DataSource<K, V, D>) super.get(dbid);
		}
	}

	@SuppressWarnings("deprecation")
	public <OK, OF extends Factor<OF>> void save(JavaSparkContext sc, JavaPairDStream<OK, OF> calculate, D detail) {
		VoidFunction<JavaPairRDD<OK, OF>> hh = saving(sc, detail);
		calculate.foreachRDD(rdd -> {
			hh.call(rdd);
			return null;
		});
	}
}
