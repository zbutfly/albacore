package net.butfly.albacore.utils;

import java.util.UUID;

import net.butfly.albacore.utils.key.IdGenerator;
import net.butfly.albacore.utils.key.ObjectIdGenerator;
import net.butfly.albacore.utils.key.SnowflakeIdGenerator;
import net.butfly.albacore.utils.key.UUIDGenerator;

public class Keys extends Utils {
	public static <K> K key(final Class<K> keyClass) {
		return Instances.fetch(new Instances.Instantiator<IdGenerator<K>>() {
			@SuppressWarnings("unchecked")
			@Override
			public synchronized IdGenerator<K> create() {
				if (String.class.equals(keyClass)) return (IdGenerator<K>) new ObjectIdGenerator();
				if (Long.class.equals(keyClass)) return (IdGenerator<K>) new SnowflakeIdGenerator();
				if (UUID.class.equals(keyClass)) return (IdGenerator<K>) new UUIDGenerator();
				return null;
			}
		}, keyClass).generate();
	}
}
