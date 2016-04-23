package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.utils.Reflections;

@SuppressWarnings("unchecked")
public abstract class Marshaller<FK, VK, VV> implements Serializable {
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

	public String parseField(Field f) {
		Reflections.noneNull("", f);
		return f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
				: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
	}
}
