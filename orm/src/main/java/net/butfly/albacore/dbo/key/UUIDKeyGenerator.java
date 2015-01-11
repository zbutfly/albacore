package net.butfly.albacore.dbo.key;

import net.butfly.albacore.utils.KeyUtils;

public class UUIDKeyGenerator extends JavaKeyGenerator<String> {
	@Override
	protected String generateKey() {
		return KeyUtils.uuid();
	}
}
