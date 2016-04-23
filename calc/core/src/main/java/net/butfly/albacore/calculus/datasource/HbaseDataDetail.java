package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class HbaseDataDetail<F> extends DataDetail<F> {
	private static final long serialVersionUID = 6027796894153816011L;

	public HbaseDataDetail(Class<F> factor, String... table) {
		super(Type.HBASE, factor, null, table);
	}
}
