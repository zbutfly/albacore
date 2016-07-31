package net.butfly.albacore.calculus.factor;

import java.io.Serializable;

import org.apache.spark.storage.StorageLevel;

import net.butfly.albacore.calculus.Calculator.Mode;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;

/**
 * Corresponds to {@code Calculating} annotation on {@code Calculus} definition.
 * 
 * @author butfly
 *
 * @param <F>
 */
public class CalculatingConfig<F extends Factor<F>> implements Serializable {
	private static final long serialVersionUID = 5323846657146326084L;
	public String key;
	public Class<F> factorClass;
	public Mode mode;
	public String dbid;
	public FactroingConfig<F> factoring;
	@Deprecated
	public long batching = 0;
	public Mechanism streaming;
	public float expanding;
	public StorageLevel persisting;
}
