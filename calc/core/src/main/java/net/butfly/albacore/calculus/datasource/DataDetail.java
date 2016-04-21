package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.common.base.Preconditions;

import net.butfly.albacore.calculus.factor.Factor.Type;

@SuppressWarnings("rawtypes")
public abstract class DataDetail<F> implements Serializable {
	private static final long serialVersionUID = 1900035964021610093L;
	public Type type;
	public String[] tables;
	public String filter;
	public Class<F> factorClass;

	protected DataDetail(Type type, Class<F> factor, String filter, String... tables) {
		Preconditions.checkArgument(tables != null && tables.length > 0);
		this.tables = tables;
		this.filter = filter;
		this.factorClass = factor;
	}

	abstract public String toString();

	public Configuration outputConfiguration(DataSource ds) {
		return HBaseConfiguration.create();
	}
}
