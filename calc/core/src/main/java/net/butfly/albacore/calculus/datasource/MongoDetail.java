package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class MongoDetail extends Detail {
	private static final long serialVersionUID = 4206637701358532787L;
	// mongodb
	public String mongoTable;
	public String mongoFilter;

	public MongoDetail(String mongoTable, String mongoFilter) {
		super(Type.MONGODB);
		this.mongoTable = mongoTable;
		this.mongoFilter = mongoFilter;
	}

	@Override
	public String toString() {
		return "[Table: " + mongoTable + ", Filter: " + mongoFilter + "]";
	}
}
