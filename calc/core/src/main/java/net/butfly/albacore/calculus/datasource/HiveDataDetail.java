package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class HiveDataDetail<F> extends DataDetail<F> {
	private static final long serialVersionUID = 6027796894153816011L;
	public boolean queryDirectly;

	public HiveDataDetail(Class<F> factor, boolean queryDirectly, String schema, String... table) {
		super(Type.HIVE, factor, null, processSchema(table, schema));
		this.queryDirectly = queryDirectly;
	}

	private static String[] processSchema(String[] table, String schema) {
		for (int i = 0; i < table.length; i++)
			if (table[i].indexOf('.') < 0) table[i] = schema + "." + table[i];
		return table;
	}
}
