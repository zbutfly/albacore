package net.butfly.albacore.calculus.factor.rds.internal;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import net.butfly.albacore.calculus.factor.Factor;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.reflect.ClassTag;

public class EncoderBuilder {
	@SuppressWarnings("unchecked")
	public static <V> Encoder<V> with(Class<?>... classes) {
		if (null == classes || classes.length == 0) {
			ClassTag<V> ct = RDSupport.tag();
			return with((Class<V>) ct.runtimeClass());
		}
		Class<?> vc = classes[0];
		if (Tuple2.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.tuple(with(classes[1]), with(classes[2]));
		if (Tuple3.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.tuple(with(classes[1]), with(classes[2]), with(classes[3]));
		if (Tuple4.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.tuple(with(classes[1]), with(classes[2]), with(classes[3]),
				with(classes[4]));
		if (Tuple5.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.tuple(with(classes[1]), with(classes[2]), with(classes[3]),
				with(classes[4]), with(classes[5]));
		if (CharSequence.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.STRING();
		if (byte[].class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.BINARY();
		if (Boolean.class.isAssignableFrom(vc) || boolean.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.BOOLEAN();
		if (Byte.class.isAssignableFrom(vc) || byte.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.BYTE();
		if (Double.class.isAssignableFrom(vc) || double.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.DOUBLE();
		if (Float.class.isAssignableFrom(vc) || float.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.FLOAT();
		if (Integer.class.isAssignableFrom(vc) || int.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.INT();
		if (Long.class.isAssignableFrom(vc) || long.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.LONG();
		if (Short.class.isAssignableFrom(vc) || short.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.SHORT();
		if (BigDecimal.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.DECIMAL();
		if (Date.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.DATE();
		if (Timestamp.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.TIMESTAMP();
		if (Factor.class.isAssignableFrom(vc)) return (Encoder<V>) Encoders.bean(vc);
		return (Encoder<V>) Encoders.kryo(vc);
	}
}
