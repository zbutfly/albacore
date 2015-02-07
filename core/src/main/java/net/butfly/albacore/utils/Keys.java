package net.butfly.albacore.utils;

import net.butfly.albacore.utils.key.IdGeneratorWrapper;

public class Keys extends Utils {
	@SuppressWarnings("unchecked")
	public static <K> K key(final Class<K> keyClass) {
		IdGeneratorWrapper<K> g = Instances.fetch(IdGeneratorWrapper.class, keyClass);
		return g.generate();
	}
}
