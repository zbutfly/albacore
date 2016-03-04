package org.apache.spark.examples;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.Functor.Stocking;
import net.butfly.albacore.calculus.Functor.Type;
import net.butfly.albacore.calculus.functor.ConstFunctor;
import net.butfly.albacore.calculus.functor.IntegerFunctor;

@Stocking(type = Type.CONST, source = "const1")
public class PiFunctor extends IntegerFunctor implements Functor<ConstFunctor<Integer>> {
	private static final long serialVersionUID = 9100426079561362807L;
	public double pi;

	public PiFunctor(String str) {
		super(str);
	}

	@Override
	public String toString() {
		return Double.toString(pi);
	}
}
