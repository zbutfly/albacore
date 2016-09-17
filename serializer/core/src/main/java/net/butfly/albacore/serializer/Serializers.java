package net.butfly.albacore.serializer;

import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.entity.ContentType;

import com.google.common.base.Charsets;

import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.Utils;

public final class Serializers extends Utils {
	public static final ContentType DEFAULT_CONTENT_TYPE = ContentType.WILDCARD.withCharset(Charsets.UTF_8);
	public static final Class<? extends Serializer<?>> DEFAULT_SERIALIZER_CLASS = scanDefaultSerializer();

	@SuppressWarnings("unchecked")
	public static Class<? extends Serializer<?>> serializerClass(String mimeType) {
		return (Class<? extends Serializer<?>>) Instances.fetch(Serializers::scanAllSerializers).get(mimeType);
	}

	@SuppressWarnings("rawtypes")
	private static Map<String, Class<? extends Serializer>> scanAllSerializers() {
		Map<String, Class<? extends Serializer>> map = new HashMap<String, Class<? extends Serializer>>();
		for (Class<? extends Serializer> serClass : Reflections.getSubClasses(Serializer.class)) {
			if (Modifier.isAbstract(serClass.getModifiers())) continue;
			Serializer<?> def = Instances.fetch(serClass, DEFAULT_CONTENT_TYPE.getMimeType());
			for (String mime : def.supportedMimeTypes())
				map.put(mime, serClass);
		}
		return map;

	}

	public static Serializer<?> serializer(final Class<? extends Serializer<?>> serializerClass, final Charset charset) {
		return Instances.fetch(serializerClass, charset);
	}

	private static final String[] INTERN_SER = new String[] { "net.butfly.albacore.serialize.HessianSerializer",
			"net.butfly.albacore.serialize.JSONSerializer" };

	@SuppressWarnings("unchecked")
	private static Class<? extends Serializer<?>> scanDefaultSerializer() {
		for (@SuppressWarnings("rawtypes")
		Class<? extends Serializer> cl : Reflections.getSubClasses(Serializer.class))
			if (!Modifier.isAbstract(cl.getModifiers()) && !cl.getName().startsWith("net.butfly.albacore.serialize."))
				return (Class<? extends Serializer<?>>) cl;
		for (String s : INTERN_SER) {
			Class<? extends Serializer<?>> cl = Reflections.forClassName(s);
			if (null != cl) return cl;
		}
		throw new RuntimeException("Could not found any serializer class implementation in classpath.");
	}

	public static boolean isSupportClass(Class<? extends TextSerializer> class1) {
		// TODO Auto-generated method stub
		return false;
	}
}
