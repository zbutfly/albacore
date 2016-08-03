package net.butfly.albacore.calculus.factor;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.common.base.Preconditions;

import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.Type;

/**
 * Corresponds to {@code Factoring} annotation on {@code Factor} definition.
 * 
 * @author butfly
 *
 * @param <V>
 */
public abstract class FactroingConfig<V> implements Serializable {
	private static final long serialVersionUID = 1900035964021610093L;
	public final Type type;
	public final String table;
	public final String query;
	public final Class<V> factorClass;
	public final String source; // db key in config

	protected FactroingConfig(Type type, Class<V> factor, String source, String table, String query) {
		Preconditions.checkArgument(table != null || query != null);
		this.type = type;
		this.source = source;
		this.table = table;
		this.query = query;
		this.factorClass = factor;
	}

	public String toString() {
		return type + " [Mapper: " + factorClass.toString() + "Source: " + table + (Factor.NOT_DEFINED.equals(query) ? ""
				: ", FactorFilter: " + query) + "]";
	}

	public Configuration outputConfiguration(@SuppressWarnings("rawtypes") DataSource ds) {
		return HBaseConfiguration.create();
	}
}
