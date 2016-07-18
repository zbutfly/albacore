package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class HiveDataDetail<F> extends DataDetail<F> {
	private static final long serialVersionUID = 6027796894153816011L;

	public HiveDataDetail(Class<F> factor, String... table) {
		super(Type.HIVE, factor, null, table);
	}
}
