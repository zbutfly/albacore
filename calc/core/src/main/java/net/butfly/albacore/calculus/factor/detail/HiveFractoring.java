package net.butfly.albacore.calculus.factor.detail;

import net.butfly.albacore.calculus.datasource.DataSource.Type;
import net.butfly.albacore.calculus.factor.FactroingConfig;

public class HiveFractoring<F> extends FactroingConfig<F> {
	private static final long serialVersionUID = 6027796894153816011L;

	public HiveFractoring(Class<F> factor, String source, String table, String query) {
		super(Type.HIVE, factor, source, table, query);
	}
}
