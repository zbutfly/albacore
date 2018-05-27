package net.butfly.albacore.utils;

import static net.butfly.albacore.utils.Configs.*;

public interface Configurated {
	default ConfigSet confLoad(String file, String prefix) {
		return of(file, this.getClass(), prefix);
	}

	default ConfigSet confLoad(String prefix) {
		return of(this.getClass(), prefix);
	}

	default String conf(String key) {
		return of(this.getClass()).get(key);
	}

	default String conf(String key, String... defs) {
		return of(this.getClass()).get(key, defs);
	}

	default String confn(String priority, String key) {
		return of(this.getClass()).getn(priority, key);
	}

	default String confn(String priority, String key, String... def) {
		return of(this.getClass()).getn(priority, key, def);
	}

	default boolean confHas(String key) {
		return of(this.getClass()).has(key);
	}
}
