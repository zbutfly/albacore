package net.butfly.albacore.utils.key;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdGeneratorWrapper<K> extends IdGenerator<K> {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	private IdGenerator<K> delegate;

	@SuppressWarnings("unchecked")
	public IdGeneratorWrapper(Class<K> keyClass) {
		if (String.class.equals(keyClass)) delegate = (IdGenerator<K>) new ObjectIdGenerator();
		else if (Long.class.equals(keyClass)) delegate = (IdGenerator<K>) new SnowflakeIdGenerator();
		else if (UUID.class.equals(keyClass)) delegate = (IdGenerator<K>) new UUIDGenerator();
		else delegate = null;
	}

	public K generate() {
		return delegate.generate();
	}

	protected long machine() {
		return delegate.machine();
	}
}