package net.butfly.albacore.calculus.factor;

import java.util.Date;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.AbstractFactors;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;

@SuppressWarnings("unchecked")
public class PairDStreamFactors extends AbstractFactors<JavaPairDStream<?, ?>> {
	private static final long serialVersionUID = 7580996793031250822L;

	public PairDStreamFactors(Calculator calc) {
		super(calc);
	}

	@Override
	protected <K, F extends Factor<F>> JavaPairDStream<K, F> dstream(JavaPairDStream<?, ?> rds) {
		return (JavaPairDStream<K, F>) rds;
	}

	@Override
	protected <K, F extends Factor<F>> JavaPairRDD<K, F> rdd(JavaPairDStream<?, ?> rds) {
		return ((JavaPairDStream<K, F>) rds).compute(new Time(new Date().getTime()));
	}

	@Override
	protected <K, F extends Factor<F>> JavaPairDStream<K, F> rds(JavaPairDStream<K, F> dstream) {
		return dstream;
	}

	@Override
	protected <K, F extends Factor<F>> JavaPairDStream<K, F> rds(JavaPairRDD<K, F> rdd) {
		return RDDDStream.pstream(calc.ssc, Mechanism.CONST, () -> rdd);
	}
}
