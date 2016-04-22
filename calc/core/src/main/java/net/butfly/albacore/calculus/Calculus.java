package net.butfly.albacore.calculus;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.factor.Factors;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.functor.Functor;
import net.butfly.albacore.calculus.utils.Logable;

public abstract class Calculus implements Logable, Serializable {
	private static final long serialVersionUID = 6432707546470042520L;
	protected final Logger logger;
	public String name;
	public Calculator calc;
	protected final Factors factors;

	public Calculus(final Calculator calc, final Factors factors) {
		logger = LoggerFactory.getLogger(getClass());
		this.calc = calc;
		this.factors = factors;
	}

	public Functor[] stages() {
		return new Functor[0];
	}

	public abstract void calculate();

	@Deprecated
	protected boolean check(PairRDS<?, ?>... rds) {
		for (PairRDS<?, ?> r : rds)
			if (r.isEmpty()) return false;
		return true;
	}
}
