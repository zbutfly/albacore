package net.butfly.albacore.calculus.datasource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.filter.HiveBuilder;
import net.butfly.albacore.calculus.factor.modifier.MapReduceKey;
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
		if (((HiveDataDetail<F>) detail).queryDirectly) {
			debug(() -> "Scaning [directly] begin: " + factor.toString() + " from table: " + detail.tables[0] + ".");
			DataFrame df = context.sql(detail.tables[0]);
			Function1<Iterator<Row>, Iterator<Tuple2<Object, F>>> f = new ScalarFunc1<Iterator<Row>, Iterator<Tuple2<Object, F>>>() {
				private static final long serialVersionUID = -8328868785608422254L;

				@Override
				public Iterator<Tuple2<Object, F>> apply(Iterator<Row> rows) {
					List<Tuple2<Object, F>> l = new ArrayList<>();
					while (rows.hasNext()) {
						Row r = rows.next();
						F f = Reflections.construct(factor);
						Object key = null;
						for (Field ff : Reflections.getDeclaredFields(factor)) {
							try {
								Reflections.set(f, ff, r.get(r.fieldIndex(ff.getName())));
							} catch (Exception ex) {}
							if (ff.isAnnotationPresent(MapReduceKey.class) && key == null) key = Reflections.get(f, ff);
						}
						l.add(new Tuple2<>(key, f));
					}
					return JavaConversions.asScalaIterator(l.iterator());
				}
			};
			return new PairRDS<Object, F>(new WrappedRDD<Tuple2<Object, F>>(df.mapPartitions(f, RDSupport.tag())));
		}

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
		DataFrame df = context.sql(hqlstr);

		Function1<Iterator<Row>, Iterator<Tuple2<Object, F>>> f = new ScalarFunc1<Iterator<Row>, Iterator<Tuple2<Object, F>>>() {
			private static final long serialVersionUID = -8328868785608422254L;

			@Override
			public Iterator<Tuple2<Object, F>> apply(Iterator<Row> rows) {
				List<Tuple2<Object, F>> l = new ArrayList<>();
				while (rows.hasNext()) {
					Row r = rows.next();
					F f = Reflections.construct(factor);
					Object key = null;
					for (Field ff : Reflections.getDeclaredFields(factor)) {
						try {
							Reflections.set(f, ff, r.get(r.fieldIndex(ff.getName())));
						} catch (Exception ex) {}
						if (ff.isAnnotationPresent(MapReduceKey.class) && key == null) key = Reflections.get(f, ff);
					}
					l.add(new Tuple2<>(key, f));
				}
				return JavaConversions.asScalaIterator(l.iterator());
			}
		};
		return new PairRDS<Object, F>(new WrappedRDD<Tuple2<Object, F>>(df.mapPartitions(f, RDSupport.tag())));

		// WrappedDataset<Object, F> ds = new WrappedDataset<Object, F>(df,
		// factor);
		// return new PairRDS<>(expandPartitions > 1 ?
		// ds.repartition(expandPartitions) : ds);
	}
}