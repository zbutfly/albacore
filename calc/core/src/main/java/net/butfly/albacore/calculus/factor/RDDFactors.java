//package net.butfly.albacore.calculus.factor;
//
//import java.util.HashMap;
//
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.bson.BSONObject;
//
//import net.butfly.albacore.calculus.Calculator;
//import net.butfly.albacore.calculus.Mode;
//import net.butfly.albacore.calculus.datasource.DataSource;
//import net.butfly.albacore.calculus.datasource.DataSource.DataSources;
//import net.butfly.albacore.calculus.datasource.HbaseDataDetail;
//import net.butfly.albacore.calculus.datasource.KafkaDataDetail;
//import net.butfly.albacore.calculus.datasource.MongoDataDetail;
//import net.butfly.albacore.calculus.factor.Factor.Stocking;
//import net.butfly.albacore.calculus.factor.Factor.Streaming;
//
//@SuppressWarnings({ "unchecked", "deprecation" })
//public final class RDDFactors extends HashMap<String, JavaPairRDD<?, ? extends Factor<?>>> {
//	private static final long serialVersionUID = -3712903710207597570L;
//	private Calculator calc;
//
//	public RDDFactors(Calculator calc, Mode mode, DataSources dss, boolean validate, Factoring... factoring) {
//		super(factoring.length);
//		this.calc = calc;
//
//		FactorConfig<?, ?> batch = null;
//		for (Factoring f : factoring) {
//			@SuppressWarnings("rawtypes")
//			Class fc = f.factor();
//			if (!fc.isAnnotationPresent(Streaming.class) && !fc.isAnnotationPresent(Stocking.class)) throw new IllegalArgumentException(
//					"Factor [" + fc.toString() + "] is annotated as neither @Streaming nor @Stocking, can't calculate it!");
//
//			FactorConfig<?, ?> c = config(mode, fc, dss, validate);
//			c.batching = f.batching();
//			c.streaming = f.stockOnStreaming();
//			// if (!fc.isAnnotationPresent(Streaming.class) && mode ==
//			// Mode.STREAMING) throw new IllegalArgumentException(
//			// "Factor [" + fc.toString() + "] is annotated as @Streaming, can't
//			// calculate it in streaming mode!");
//			if (f.batching() > 0) {
//				if (batch != null) throw new IllegalArgumentException("Only one batch stocking source supported, now found second: "
//						+ batch.factorClass.toString() + " and " + c.factorClass.toString());
//				else batch = c;
//			}
//			read(mode, f.key(), c);
//		}
//	}
//
//	public <K, F extends Factor<F>> void stocking(String factoring, JavaPairRDD<K, F> rdd) {
//		if (this.containsKey(factoring)) throw new IllegalArgumentException("Conflictted factoring id: " + factoring);
//		rdd.setName("RDD [" + factoring + "]");
//		this.put(factoring, rdd);
//	}
//
//	public <K, F extends Factor<F>> JavaPairRDD<K, F> stocking(String factoring) {
//		return (JavaPairRDD<K, F>) get(factoring);
//	}
//
//	private <K, F extends Factor<F>> void read(Mode mode, String key, FactorConfig<K, F> config) {
//		if (mode == Mode.STOCKING && config.batching <= 0)
//			this.stocking(key, calc.dss.ds(config.dbid).stocking(calc.sc, config.factorClass, config.detail));
//	}
//}
