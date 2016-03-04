package net.butfly.albacore.dbo.key;

import net.butfly.albacore.utils.Keys;

public class ObjectIdKeyGenerator extends JavaKeyGenerator<String> {
	@Override
	protected String generateKey() {
		return Keys.key(String.class);
	}
}
