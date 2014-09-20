package net.butfly.albacore.cache.utils.strategy.keygenerate;

import net.butfly.albacore.cache.utils.Key;

public interface IKeyGenerator {
	String getKey(Key key);
}
