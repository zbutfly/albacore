package net.butfly.albacore.calculus.datasource;

import java.lang.reflect.Field;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.storm.guava.collect.ImmutableMap;
import org.elasticsearch.spark.rdd.EsSpark;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.rds.RDS;
import net.butfly.albacore.calculus.marshall.Marshaller;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Map;

@SuppressWarnings("rawtypes")
public class ElasticDataSource extends DataSource<String, String, Map, String, Object> {
	private static final long serialVersionUID = 5990012673598725014L;
	public final String baseUrl;

	public ElasticDataSource(String baseUrl, boolean validate) {
		super(Type.ELASTIC, validate, new Marshaller<String, String, Map>(), String.class, Map.class, OutputFormat.class, null);
		this.baseUrl = baseUrl;
	}

	@Override
	public <V> Tuple2<String, Object> beforeWriting(String key, V value) {
		if (null == value) return null;
		Field f = ElasticDataDetail.findId(value.getClass());
		if (null != f) {
			f.setAccessible(true);
			try {
				f.set(value, key);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				error(() -> "Error in id setting", e);
			}
		}
		return new Tuple2<>(null, value);
	}

	@Override
	public void save(JavaPairRDD<String, Object> rdd, DataDetail<?> dd) {
		EsSpark.saveToEs(rdd.values().rdd(), dd.tables[0], JavaConverters.asScalaMapConverter(ImmutableMap.of("es.mapping.id",
				((ElasticDataDetail) dd).idField)).asScala());
	}

	@Override
	public <F extends Factor<F>> JavaPairRDD<String, F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail,
			FactorFilter... filters) {
		JavaPairRDD<String, Map<String, Object>> r = JavaPairRDD.fromRDD(EsSpark.esRDD(calc.sc.sc(), baseUrl + detail.tables[0], filter(
				detail.filter, filters)), RDS.tag(), RDS.tag());
		return r.mapToPair((Tuple2<String, Map<String, Object>> t) -> afterReading(t._1, t._2, factor));
	}

	private String filter(String filter, FactorFilter[] filters) {
		return null;
	}
}