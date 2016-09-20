package net.butfly.albacore.serder;

import net.butfly.albacore.utils.CaseFormat;

public abstract class BeanSerderBase<DATA> implements BeanSerder<DATA> {
	private static final long serialVersionUID = -1444796714401247902L;
	private CaseFormat format;

	public BeanSerderBase() {
		this(null);
	}

	public BeanSerderBase(CaseFormat format) {
		super();
		this.format = format;
	}

	@Override
	public CaseFormat mappingFormat() {
		return format;
	}
}
