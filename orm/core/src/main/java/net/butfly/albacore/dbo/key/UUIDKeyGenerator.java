package net.butfly.albacore.dbo.key;

import java.util.UUID;

import net.butfly.albacore.utils.Keys;

public class UUIDKeyGenerator extends JavaKeyGenerator<String> {
	@Override
	protected String generateKey() {
		return Keys.key(UUID.class).toString();
	}
}
