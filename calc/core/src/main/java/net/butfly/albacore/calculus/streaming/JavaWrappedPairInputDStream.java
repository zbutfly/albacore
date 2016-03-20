package net.butfly.albacore.calculus.streaming;

import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.dstream.InputDStream;

import scala.Tuple2;
import scala.reflect.ManifestFactory;

public abstract class JavaWrappedPairInputDStream<K, V, S extends InputDStream<Tuple2<K, V>>> extends JavaPairInputDStream<K, V> {
	private static final long serialVersionUID = -4050866241323957492L;

	public JavaWrappedPairInputDStream(S inputDStream, Class<K> kClass, Class<V> vClass) {
		super(inputDStream, ManifestFactory.classType(kClass), ManifestFactory.classType(vClass));
	}

	@SuppressWarnings("unchecked")
	public long counts() {
		return ((WrappedPairInputDStream<K, V>) super.dstream()).current.count();
	}
}
