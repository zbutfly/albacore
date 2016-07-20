package net.butfly.albacore.calculus.datasource;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.filter.HiveBuilder;
import net.butfly.albacore.calculus.factor.modifier.Key;
import net.butfly.albacore.calculus.marshall.HiveMarshaller;
import scala.Tuple2;

public class HiveDataSource extends DataSource<Object, Row, Row, Object, Row> {
	private static final long serialVersionUID = 2229814193461610013L;
	public final HiveContext context;
	public final String schema;

	public HiveDataSource(String schema, HiveMarshaller marshaller, JavaSparkContext sc) {
		super(Type.HIVE, false, null == marshaller ? new HiveMarshaller() : marshaller, Row.class, Row.class, null, null);
		this.schema = schema;
		this.context = new HiveContext(sc.sc());
	}

	@Override
	public String toString() {
		return super.toString();
	}

	@Override
	public <F extends Factor<F>> JavaPairRDD<Object, F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail,
			float expandPartitions, FactorFilter... filters) {
		if (calc.debug && debugLimit > 0 && debugRandomChance > 0) filters = enableDebug(filters);
		debug(() -> "Scaning begin: " + factor.toString() + " from table: " + detail.tables[0] + ".");
		StringBuilder hql = new StringBuilder("select * from ").append(detail.tables[0]);
		HiveBuilder<F> b = new HiveBuilder<F>(factor, (HiveMarshaller) marshaller);
		String q = b.filter(filters);
		if (null != q) hql.append(q);
		String hqlstr = b.finalize(hql).toString();
		debug(() -> "Hive HQL parsed into: \n\t" + hqlstr);
		DataFrame df = this.context.sql(hqlstr);

		JavaRDD<Row> rows = df.javaRDD();
		if (expandPartitions > 1) rows = rows.repartition((int) Math.ceil(rows.getNumPartitions() * expandPartitions));
		@SuppressWarnings("unchecked")
		String key = ((HiveMarshaller) marshaller).parseQualifier(factor, marshaller.parse(factor, Key.class)._1, df.schema().fieldNames());
		return rows.mapToPair(row -> new Tuple2<Object, F>(row.get(row.fieldIndex(key)), marshaller.unmarshall(row, factor)));
	}
}