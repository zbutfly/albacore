package net.butfly.albacore.calculus.factor;

import java.io.Serializable;

import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.datasource.Detail;

public class FactorConfig<F extends Factor<F>> implements Serializable {
	private static final long serialVersionUID = 5323846657146326084L;
	public Class<F> factorClass;
	public Mode mode;
	public String dbid;
	public Detail detail;
}
