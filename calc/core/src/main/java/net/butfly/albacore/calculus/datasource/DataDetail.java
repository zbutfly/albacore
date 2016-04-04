package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;

import com.google.common.base.Preconditions;

import net.butfly.albacore.calculus.factor.Factor.Type;

public abstract class DataDetail implements Serializable {
	private static final long serialVersionUID = 1900035964021610093L;
	public Type type;
	public String[] tables;
	public String filter;

	protected DataDetail(Type type, String filter, String... tables) {
		Preconditions.checkArgument(tables != null && tables.length > 0);
		this.tables = tables;
		this.filter = filter;
	}

	abstract public String toString();
}
