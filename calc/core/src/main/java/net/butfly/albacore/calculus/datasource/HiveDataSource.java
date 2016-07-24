package net.butfly.albacore.calculus.datasource;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.filter.HiveBuilder;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedDataset;
import net.butfly.albacore.calculus.marshall.RowMarshaller;

public class HiveDataSource extends DataSource<Object, Row, Row, Object, Row> {
	private static final long serialVersionUID = 2229814193461610013L;
	public final HiveContext context;
	public final String schema;

	public HiveDataSource(String schema, JavaSparkContext sc, CaseFormat srcFormat, CaseFormat dstFormat) {
		super(Type.HIVE, false, RowMarshaller.class, Row.class, Row.class, null, null, srcFormat, dstFormat);
		this.schema = schema;
		this.context = new HiveContext(sc.sc());
	}

	@Override
	public String toString() {
		return super.toString();
	}

	@Override
	public <F extends Factor<F>> PairRDS<Object, F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail, float expandPartitions,
			FactorFilter... filters) {
		if (calc.debug) filters = enableDebug(filters);
		debug(() -> "Scaning begin: " + factor.toString() + " from table: " + detail.tables[0] + ".");
		StringBuilder hql = new StringBuilder("select ");
		((RowMarshaller) marshaller).colsAsFields(hql, factor);
		hql.append(" from ").append(detail.tables[0]);
		HiveBuilder<F> b = new HiveBuilder<F>(factor, (RowMarshaller) marshaller);
		String q = b.filter(filters);
		if (null != q) hql.append(q);
		String hqlstr = b.finalize(hql).toString();
		debug(() -> "Hive HQL parsed into: \n\t" + hqlstr);
		WrappedDataset<Object, F> ds = new WrappedDataset<Object, F>(context.sql(hqlstr), factor);
		return new PairRDS<>(expandPartitions > 1 ? ds.repartition(expandPartitions) : ds);
	}
}