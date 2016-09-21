package net.butfly.albacore.serder;

import net.butfly.albacore.utils.CaseFormat;

public abstract class BeanSerderBase<DATA> implements BeanSerder<DATA> {
	private static final long serialVersionUID = -1444796714401247902L;
	private CaseFormat format = CaseFormat.NO_CHANGE;

	@Override
	public BeanSerder<DATA> mapping(CaseFormat to) {
		this.format = to;
		return this;
	}

	@Override
	public CaseFormat mapping() {
		return format;
	}
}
