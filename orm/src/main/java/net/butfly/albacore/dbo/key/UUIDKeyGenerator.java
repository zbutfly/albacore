package net.butfly.albacore.dbo.key;

import java.util.UUID;

public class UUIDKeyGenerator extends JavaKeyGenerator<UUID> {
	@Override
	protected UUID generateKey() {
		return UUID.randomUUID();
	}
}
