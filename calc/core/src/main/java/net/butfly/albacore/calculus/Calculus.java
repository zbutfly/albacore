package net.butfly.albacore.calculus;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factors;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.functor.Functor;

public abstract class Calculus<OK, OF extends Factor<OF>> implements Serializable {
	private static final long serialVersionUID = 6432707546470042520L;
	public String name;
	public Calculator calc;
	protected final Logger logger;

	public Calculus() {
		this.logger = LoggerFactory.getLogger(this.getClass());
	}

	public Functor[] stages() {
		return new Functor[0];
	}

	abstract public PairRDS<OK, OF> calculate(final Factors factors);

	protected boolean saving(JavaPairRDD<OK, OF> r) {
		return true;
	}

	protected boolean check(PairRDS<?, ?>... rds) {
		for (PairRDS<?, ?> r : rds)
			if (r.isEmpty()) return false;
		return true;
	}

	final Calculus<OK, OF> calculator(Calculator calculator) {
		this.calc = calculator;
		return this;
	}
}
