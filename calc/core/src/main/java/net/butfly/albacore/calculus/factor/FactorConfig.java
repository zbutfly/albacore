package net.butfly.albacore.calculus.factor;

import java.io.Serializable;

import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;

public class FactorConfig<K, F extends Factor<F>> implements Serializable {
	private static final long serialVersionUID = 5323846657146326084L;
	public String id;
	public Class<K> keyClass;
	public Class<F> factorClass;
	public Mode mode;
	public String dbid;
	public DataDetail<F> detail;
	@Deprecated
	public long batching = 0;
	public Mechanism streaming;
}
