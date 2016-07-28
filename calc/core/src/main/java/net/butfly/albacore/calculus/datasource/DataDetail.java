package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.common.base.Preconditions;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;

public abstract class DataDetail<V> implements Serializable {
	private static final long serialVersionUID = 1900035964021610093L;
	public final Type type;
	public final String[] tables;
	public final String filter;
	public final Class<V> factorClass;
	public final String source; // db key in config

	protected DataDetail(Type type, Class<V> factor, String source, String filter, String... tables) {
		Preconditions.checkArgument(tables != null && tables.length > 0);
		this.type = type;
		this.source = source;
		this.tables = tables;
		this.filter = filter;
		this.factorClass = factor;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(tables[0]);
		for (int i = 1; i < tables.length; i++)
			sb.append(",").append(tables[i]);

		return type + " [Mapper: " + factorClass.toString() + "Source: " + sb.toString()
				+ (Factor.NOT_DEFINED.equals(filter) ? "" : ", FactorFilter: " + filter) + "]";
	}

	public Configuration outputConfiguration(@SuppressWarnings("rawtypes") DataSource ds) {
		return HBaseConfiguration.create();
	}
}
