package net.butfly.albacore.serder;

import net.butfly.albacore.utils.CaseFormat;

public abstract class BeanSerderBase<P, DATA> implements BeanSerder<P, DATA> {
	private static final long serialVersionUID = -1444796714401247902L;
	private CaseFormat format = CaseFormat.ORIGINAL;

	@Override
	public BeanSerder<P, DATA> mapping(CaseFormat to) {
		this.format = to;
		return this;
	}

	@Override
	public CaseFormat mapping() {
		return format;
	}
}
