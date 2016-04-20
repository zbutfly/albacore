package net.butfly.albacore.calculus;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factors;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.functor.Functor;
import net.butfly.albacore.calculus.utils.Logable;

public abstract class Calculus<OK, OF extends Factor<OF>> implements Logable, Serializable {
	private static final long serialVersionUID = 6432707546470042520L;
	protected final Logger logger;
	public String name;
	public Calculator calc;
	protected final Factors factors;

	public Calculus(final Factors factors) {
		logger = LoggerFactory.getLogger(getClass());
		this.factors = factors;
	}

	public Functor[] stages() {
		return new Functor[0];
	}

	public abstract PairRDS<OK, OF> calculate();

	protected boolean saving(JavaPairRDD<OK, OF> rdd) {
		return true;
	}

	@Deprecated
	protected boolean check(PairRDS<?, ?>... rds) {
		for (PairRDS<?, ?> r : rds)
			if (r.isEmpty()) return false;
		return true;
	}

	final Calculus<OK, OF> calculator(Calculator calculator) {
		calc = calculator;
		return this;
	}
}
