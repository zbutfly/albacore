package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class MongoDataDetail extends DataDetail {
	private static final long serialVersionUID = 4206637701358532787L;
	// mongodb
	public String mongoTable;
	public String mongoFilter;

	public MongoDataDetail(String[] mongoTables, String mongoFilter) {
		super(Type.MONGODB);
		// TODO: support multeple tables
		this.mongoTable = mongoTables[0];
		this.mongoFilter = mongoFilter;
	}

	@Override
	public String toString() {
		return "[Table: " + mongoTable + ", Filter: " + mongoFilter + "]";
	}
}
