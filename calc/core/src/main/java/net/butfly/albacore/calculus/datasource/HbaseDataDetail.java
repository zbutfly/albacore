package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class HbaseDataDetail extends DataDetail {
	private static final long serialVersionUID = 6027796894153816011L;

	public HbaseDataDetail(String... table) {
		super(Type.HBASE, null, table);
	}

	@Override
	public String toString() {
		return "[Table: " + this.tables[0] + "]";
	}
}
