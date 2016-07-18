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
import net.butfly.albacore.calculus.marshall.HiveMarshaller;
import scala.Tuple2;

public class HiveDataSource extends DataSource<Object, Object, Row, Object, Row> {
	private static final long serialVersionUID = 2229814193461610013L;
	final String configFile;
	final HiveContext context;

	public HiveDataSource(String configFile, HiveMarshaller marshaller, JavaSparkContext sc) {
		super(Type.HIVE, false, null == marshaller ? new HiveMarshaller() : marshaller, Object.class, Row.class, null, null);
		this.configFile = configFile;
		this.context = new HiveContext(sc.sc());
	}

	@Override
	public String toString() {
		return super.toString() + ":" + this.configFile;
	}

	public String getConfigFile() {
		return configFile;
	}

	@Override
	public <F extends Factor<F>> JavaPairRDD<Object, F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail,
			float expandPartitions, FactorFilter... filters) {
		if (calc.debug && debugLimit > 0 && debugRandomChance > 0) filters = enableDebug(filters);
		debug(() -> "Scaning begin: " + factor.toString() + " from table: " + detail.tables[0] + ".");
		StringBuilder hql = new StringBuilder("select * from ").append(detail.tables[0]);
		if (filters.length > 0) {
			hql.append(" where ");
			for (FactorFilter f : filters)
				hql.append(filter(f));
		}
		DataFrame df = this.context.sql(hql.toString());
		JavaRDD<Row> rows = df.javaRDD();
		if (expandPartitions > 1) rows = rows.repartition((int) Math.ceil(rows.getNumPartitions() * expandPartitions));
		return rows.mapToPair(t -> new Tuple2<Object, F>(null, null));
	}

	private String filter(FactorFilter f) {
		// TODO Auto-generated method stub
		return null;
	}
}