package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class HiveDataDetail<F> extends DataDetail<F> {
	private static final long serialVersionUID = 6027796894153816011L;

	public HiveDataDetail(Class<F> factor, String source, String schema, String... table) {
		super(Type.HIVE, factor, source, null, processSchema(table, schema));
	}

	private static String[] processSchema(String[] table, String schema) {
		for (int i = 0; i < table.length; i++)
			if (table[i].indexOf('.') < 0) table[i] = schema + "." + table[i];
		return table;
	}
}
