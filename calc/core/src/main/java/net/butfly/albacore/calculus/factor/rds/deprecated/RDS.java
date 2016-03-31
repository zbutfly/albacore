package net.butfly.albacore.calculus.factor.rds.deprecated;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;

@Deprecated
public class RDS<T> extends AbstractRDS<T, JavaRDD<T>, JavaDStream<T>> implements Serializable {
	private static final long serialVersionUID = 1908771862271821691L;

	protected RDS() {}

	@SafeVarargs
	public RDS(JavaRDD<T>... rdd) {
		super(rdd);
	}

	public RDS(JavaDStream<T> dstream) {
		super(dstream);
	}

	@SafeVarargs
	public RDS(JavaSparkContext sc, T... t) {
		this(sc.parallelize(Arrays.asList(t)));
	}

	public RDS<T> folk() {
		switch (type) {
		case RDD:
			for (int i = 0; i < rdds.length; i++)
				rdds[i] = rdds[i].cache();
			return new RDS<T>(rdds);
		case DSTREAM:
			dstream = dstream.cache();
			return new RDS<T>(dstream);
		}
		return this;
	}
}
