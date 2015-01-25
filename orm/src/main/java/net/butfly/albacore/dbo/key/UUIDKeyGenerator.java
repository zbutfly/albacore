package net.butfly.albacore.dbo.key;

import net.butfly.albacore.utils.Keys;

public class UUIDKeyGenerator extends JavaKeyGenerator<String> {
	@Override
	protected String generateKey() {
		return Keys.uuid();
	}
}
