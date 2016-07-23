package net.butfly.albacore.calculus.datasource;

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
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedDataFrame;
import net.butfly.albacore.calculus.marshall.RowMarshaller;

public class HiveDataSource extends DataSource<Object, Row, Row, Object, Row> {
	private static final long serialVersionUID = 2229814193461610013L;
	public final HiveContext context;
	public final String schema;

	public HiveDataSource(String schema, RowMarshaller marshaller, JavaSparkContext sc) {
		super(Type.HIVE, false, null == marshaller ? new RowMarshaller() : marshaller, Row.class, Row.class, null, null);
		this.schema = schema;
		this.context = new HiveContext(sc.sc());
	}

	@Override
	public String toString() {
		return super.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <F extends Factor<F>> PairRDS<Object, F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail,
			float expandPartitions, FactorFilter... filters) {
		if (calc.debug) filters = enableDebug(filters);
		debug(() -> "Scaning begin: " + factor.toString() + " from table: " + detail.tables[0] + ".");
		StringBuilder hql = new StringBuilder("select * from ").append(detail.tables[0]);
		HiveBuilder<F> b = new HiveBuilder<F>(factor, (RowMarshaller) marshaller);
		String q = b.filter(filters);
		if (null != q) hql.append(q);
		String hqlstr = b.finalize(hql).toString();
		debug(() -> "Hive HQL parsed into: \n\t" + hqlstr);
		DataFrame df = this.context.sql(hqlstr);

		String key = ((RowMarshaller) marshaller).parseQualifier(factor, marshaller.parse(factor, Key.class)._1, df.schema().fieldNames());
		df = df.repartition(df.col(key));
		if (expandPartitions > 1) df = df.repartition((int) Math.ceil(df.javaRDD().getNumPartitions() * expandPartitions));
//		RDD<Tuple2<Object, F>> rdd = df.mapPartitions(new AbstractFunction1<Iterator<Row>, Iterator<Tuple2<Object, F>>>() {
//			@Override
//			public Iterator<Tuple2<Object, F>> apply(Iterator<Row> rows) {
//				List<Tuple2<Object, F>> list = new ArrayList<>();
//				while (rows.hasNext()) {
//					Row r = rows.next();
//					list.add(new Tuple2<Object, F>(r.get(r.fieldIndex(key)), marshaller.unmarshall(r, factor)));
//				}
//				return JavaConversions.asScalaIterator(list.iterator());
//			}
//		}, RDSupport.tag());
		return new PairRDS<>(new WrappedDataFrame<>(df, Object.class, factor));
	}
}