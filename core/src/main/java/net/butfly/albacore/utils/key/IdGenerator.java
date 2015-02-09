package net.butfly.albacore.utils.key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IdGenerator<K> {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	public abstract K generate();

	protected abstract long machine();
}