package net.butfly.albacore.calculus.factor.filter;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.marshall.Marshaller;

public abstract class Builder<T, E, F extends Factor<F>> implements Serializable {
	private static final long serialVersionUID = -2671959595644176197L;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected final Class<F> factor;
	@SuppressWarnings("rawtypes")
	protected final Marshaller marshaller;

	public Builder(Class<F> factor, @SuppressWarnings("rawtypes") Marshaller marshaller) {
		super();
		this.factor = factor;
		this.marshaller = marshaller;
	}

	abstract public T filter(FactorFilter... filter);

	abstract protected T filterOne(FactorFilter filter);

	abstract protected E expression(Class<?> type, Object value);
}
