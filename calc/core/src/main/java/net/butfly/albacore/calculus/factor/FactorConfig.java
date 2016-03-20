package net.butfly.albacore.calculus.factor;

import java.io.Serializable;

import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.factor.Factor.Stocking.OnStreaming;

public class FactorConfig<K, F extends Factor<F>> implements Serializable {
	private static final long serialVersionUID = 5323846657146326084L;
	public Class<K> keyClass;
	public Class<F> factorClass;
	public Mode mode;
	public String dbid;
	public DataDetail detail;
	public long batching = 0;
	public OnStreaming streaming;
}
