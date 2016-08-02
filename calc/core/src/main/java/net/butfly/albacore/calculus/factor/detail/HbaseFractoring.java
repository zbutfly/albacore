package net.butfly.albacore.calculus.factor.detail;

import net.butfly.albacore.calculus.datasource.DataSource.Type;
import net.butfly.albacore.calculus.factor.FactroingConfig;

public class HbaseFractoring<F> extends FactroingConfig<F> {
	private static final long serialVersionUID = 6027796894153816011L;

	public HbaseFractoring(Class<F> factor, String source, String table) {
		super(Type.HBASE, factor, source, table, null);
	}
}
