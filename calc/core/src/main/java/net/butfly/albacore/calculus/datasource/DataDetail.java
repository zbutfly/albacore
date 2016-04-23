package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.guava.base.Joiner;

import com.google.common.base.Preconditions;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;

@SuppressWarnings("rawtypes")
public abstract class DataDetail<V> implements Serializable {
	private static final long serialVersionUID = 1900035964021610093L;
	public Type type;
	public String[] tables;
	public String filter;
	public Class<V> factorClass;

	protected DataDetail(Type type, Class<V> factor, String filter, String... tables) {
		Preconditions.checkArgument(tables != null && tables.length > 0);
		this.tables = tables;
		this.filter = filter;
		this.factorClass = factor;
	}

	public String toString() {
		return type + " [Mapper: " + factorClass.toString() + "Source: " + Joiner.on(',').join(tables) + (Factor.NOT_DEFINED.equals(filter)
				? "" : ", FactorFilter: " + filter) + "]";
	}

	public Configuration outputConfiguration(DataSource ds) {
		return HBaseConfiguration.create();
	}
}
