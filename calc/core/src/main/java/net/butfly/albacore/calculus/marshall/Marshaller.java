package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.factor.Factor;

@SuppressWarnings("unchecked")
public abstract class Marshaller<FK, VK, VV> implements Serializable {
	private static final long serialVersionUID = 6678021328832491260L;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	public FK unmarshallId(VK id) {
		return null == id ? null : (FK) id;
	}

	public <T extends Factor<T>> T unmarshall(VV from, Class<T> to) {
		return (T) from;
	}

	public VK marshallId(FK id) {
		return (VK) id;
	}

	public <T extends Factor<T>> VV marshall(T from) {
		return (VV) from;
	}
}
