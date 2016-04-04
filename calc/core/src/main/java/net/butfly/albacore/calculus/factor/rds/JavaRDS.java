//package net.butfly.albacore.calculus.factor.rds;
//
//import java.io.Serializable;
//import java.util.Arrays;
//
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.streaming.api.java.JavaDStream;
//
//@Deprecated
//public class JavaRDS<T> extends JavaRDSBase<T, JavaRDD<T>, JavaDStream<T>> implements Serializable {
//	private static final long serialVersionUID = 1908771862271821691L;
//
//	protected JavaRDS() {}
//
//	@SafeVarargs
//	public JavaRDS(JavaRDD<T>... rdd) {
//		super(rdd);
//	}
//
//	public JavaRDS(JavaDStream<T> dstream) {
//		super(dstream);
//	}
//
//	@SafeVarargs
//	public JavaRDS(JavaSparkContext sc, T... t) {
//		this(sc.parallelize(Arrays.asList(t)));
//	}
//
//	public JavaRDS<T> folk() {
//		switch (type) {
//		case RDD:
//			for (int i = 0; i < rdds.length; i++)
//				rdds[i] = rdds[i].cache();
//			return new JavaRDS<T>(rdds);
//		case DSTREAM:
//			dstream = dstream.cache();
//			return new JavaRDS<T>(dstream);
//		}
//		return this;
//	}
//}
