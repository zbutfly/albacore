package net.butfly.albacore.dbo.key;

import org.bson.types.ObjectId;

public class ObjectIdKeyGenerator extends JavaKeyGenerator<String> {
	@Override
	protected String generateKey() {
		return new ObjectId().toHexString();
	}
}
