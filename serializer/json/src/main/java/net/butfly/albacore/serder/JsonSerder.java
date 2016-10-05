package net.butfly.albacore.serder;

import net.butfly.albacore.utils.CaseFormat;

public class JsonSerder extends JsonEntitySerder<Object> {
	private static final long serialVersionUID = -3006552685492072400L;

	@Override
	public JsonSerder mapping(CaseFormat to) {
		super.mapping(to);
		return this;
	}
}
