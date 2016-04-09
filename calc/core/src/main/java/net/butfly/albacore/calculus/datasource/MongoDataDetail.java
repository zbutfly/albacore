package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class MongoDataDetail extends DataDetail {
	private static final long serialVersionUID = 4206637701358532787L;

	public MongoDataDetail(String filter, String... table) {
		super(Type.MONGODB, filter, table);
	}

	@Override
	public String toString() {
		return "[Table: " + tables[0] + ", Filter: " + filter + "]";
	}
}
