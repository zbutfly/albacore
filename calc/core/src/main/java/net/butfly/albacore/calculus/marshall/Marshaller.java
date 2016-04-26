package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;
import scala.Tuple3;

@SuppressWarnings("unchecked")
public class Marshaller<FK, VK, VV> implements Serializable {
	private static final long serialVersionUID = 6678021328832491260L;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	public FK unmarshallId(VK id) {
		return null == id ? null : (FK) id;
	}

	public <T> T unmarshall(VV from, Class<T> to) {
		return (T) from;
	}

	public VK marshallId(FK id) {
		return (VK) id;
	}

	public <T> VV marshall(T from) {
		return (VV) from;
	}

	public Comparator<FK> comparator() {
		throw new UnsupportedOperationException();
	}

	public String parseQualifier(Field f) {
		Reflections.noneNull("", f);
		return f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
				: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
	}

	public <V> Tuple3<Field, String, ? extends Annotation> parse(Class<V> c, Class<? extends Annotation>... annotation) {
		for (Field f : Reflections.getDeclaredFields(c))
			for (Class<? extends Annotation> a : annotation)
				if (f.isAnnotationPresent(a)) return new Tuple3<>(f, parseQualifier(f), f.getAnnotation(a));
		return null;
	}

	public <V> Map<Field, Tuple2<String, ? extends Annotation>> parseAll(Class<V> c, Class<? extends Annotation>... annotation) {
		Map<Field, Tuple2<String, ? extends Annotation>> fs = new HashMap<>();
		for (Field f : Reflections.getDeclaredFields(c))
			for (Class<? extends Annotation> a : annotation)
				if (f.isAnnotationPresent(a)) {
					Tuple2<String, ? extends Annotation> t = new Tuple2<>(parseQualifier(f), f.getAnnotation(a));
					fs.put(f, t);
					break;
				}
		return fs;
	}
}
