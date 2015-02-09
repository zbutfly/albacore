package net.butfly.albacore.utils.key;

import java.util.UUID;

/**
 * @author butfly
 */
public class UUIDGenerator extends IdGenerator<UUID> {

	@Override
	public UUID generate() {
		return UUID.randomUUID();
	}

	@Override
	protected long machine() {
		throw new UnsupportedOperationException();
	}
}