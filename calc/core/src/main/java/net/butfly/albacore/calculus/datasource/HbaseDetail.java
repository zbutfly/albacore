package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class HbaseDetail extends Detail {
	private static final long serialVersionUID = 6027796894153816011L;
	// hbase conf
	public String hbaseTable;

	public HbaseDetail(String hbaseTable) {
		super(Type.HBASE);
		this.hbaseTable = hbaseTable;
	}

	@Override
	public String toString() {
		return "[Table: " + hbaseTable + "]";
	}
}
