package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class HbaseDataDetail extends DataDetail {
	private static final long serialVersionUID = 6027796894153816011L;
	// hbase conf
	public String hbaseTable;

	public HbaseDataDetail(String[] hbaseTables) {
		super(Type.HBASE);
		this.hbaseTable = hbaseTables[0];
	}

	@Override
	public String toString() {
		return "[Table: " + hbaseTable + "]";
	}
}
