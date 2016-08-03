package net.butfly.albacore.calculus.datasource;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.FactroingConfig;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.filter.HiveBuilder;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.factor.rds.internal.RDSupport;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedRDD;
import net.butfly.albacore.calculus.lambda.ScalarFunc1;
import net.butfly.albacore.calculus.marshall.RowMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;

public class HiveDataSource extends DataSource<Object, Row, Row, Object, Row> {
	private static final long serialVersionUID = 2229814193461610013L;
	private final transient SparkContext sc;
	private HiveContext context = null;

	public HiveDataSource(String schema, JavaSparkContext sc, CaseFormat srcFormat, CaseFormat dstFormat) {
		super(Type.HIVE, schema, false, RowMarshaller.class, Row.class, Row.class, null, null, srcFormat, dstFormat);
		this.sc = sc.sc();
	}

	@Override
	public String toString() {
		return super.toString();
	}

	@Override
	public <F extends Factor<F>> PairRDS<Object, F> stocking(Class<F> factor, FactroingConfig<F> detail, float expandPartitions,
			FactorFilter... filters) {
		Function1<Iterator<Row>, Iterator<Tuple2<Object, F>>> f = new ScalarFunc1<Iterator<Row>, Iterator<Tuple2<Object, F>>>() {
			private static final long serialVersionUID = -8328868785608422254L;

			@Override
			public Iterator<Tuple2<Object, F>> apply(Iterator<Row> rows) {
				return JavaConversions.asScalaIterator(Reflections.transform(JavaConversions.asJavaIterator(rows),
						r -> new Tuple2<Object, F>(marshaller.unmarshallId(r), marshaller.unmarshall(r, factor))).iterator());
			}
		};
		if (Calculator.calculator.debug) filters = enableDebug(filters);
		String q = detail.table == null ? detail.query : detail.table;
		trace(() -> "Scaning begin: " + factor.toString() + " from query: " + q + ".");
		if (null == context) context = new HiveContext(sc);
		DataFrame df = context.sql(q);
		// df = context.sql(hql(factor, detail, filters));
		return new PairRDS<Object, F>(new WrappedRDD<Tuple2<Object, F>>(df.mapPartitions(f, RDSupport.tag())));
	}

	protected <F extends Factor<F>> String hql(Class<F> factor, FactroingConfig<F> detail, FactorFilter... filters) {
		debug(() -> "Scaning begin: " + factor.toString() + " from table: " + detail.table + ".");
		StringBuilder hql = new StringBuilder("select ");
		((RowMarshaller) marshaller).colsAsFields(hql, factor);
		hql.append(" from ").append(detail.table);
		HiveBuilder<F> b = new HiveBuilder<F>(factor, (RowMarshaller) marshaller);
		String q = b.filter(filters);
		if (null != q) hql.append(q);
		String hqlstr = b.finalize(hql).toString();
		debug(() -> "Hive HQL parsed into: \n\t" + hqlstr);
		return hqlstr;
	}

	private static final Joiner AND_JOINER = Joiner.on(" ) and (");

	@Override
	public String andQuery(String... ands) {
		if (ands == null || ands.length == 0) return null;
		if (ands.length == 1) return ands[0];
		return "(" + AND_JOINER.join(ands) + ")";
	}
}