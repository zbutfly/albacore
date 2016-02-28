package net.butfly.albacore.calculus;

public @interface Stocking {
	public enum Type {
		HBASE, MONGODB
	}

	Type type();

	String db();

	String table();
}
