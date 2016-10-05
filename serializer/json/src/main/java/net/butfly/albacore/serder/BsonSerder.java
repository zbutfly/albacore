package net.butfly.albacore.serder;

import net.butfly.albacore.utils.CaseFormat;

public class BsonSerder extends BsonEntitySerder<Object> {
	private static final long serialVersionUID = 6422217000021925664L;

	@Override
	public BsonSerder mapping(CaseFormat to) {
		super.mapping(to);
		return this;
	}
}
