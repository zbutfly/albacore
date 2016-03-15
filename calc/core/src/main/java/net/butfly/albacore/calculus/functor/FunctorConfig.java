package net.butfly.albacore.calculus.functor;

import java.io.Serializable;

import net.butfly.albacore.calculus.Calculating;
import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.datasource.Detail;

public class FunctorConfig<F extends Functor<F>> implements Serializable {
	private static final long serialVersionUID = 5323846657146326084L;
	public Class<F> functorClass;
	public Mode mode;
	public String dbid;
	public Detail detail;
}
