package net.butfly.albacore.serializer;

import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.http.entity.ContentType;

import com.google.common.base.Charsets;

import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.Utils;

public final class Serializers extends Utils {
	public static final ContentType DEFAULT_CONTENT_TYPE = ContentType.WILDCARD.withCharset(Charsets.UTF_8);
	public static final Class<? extends Serializer<?>> DEFAULT_SERIALIZER_CLASS = scanDefaultSerializer();

	@SuppressWarnings("unchecked")
	public static Class<? extends ContentSerializer<?>> contentSerializers(String mimeType) {
		return (Class<? extends ContentSerializer<?>>) Instances.fetch(Serializers::scanContentSerializers).get(mimeType);
	}

	@SuppressWarnings("rawtypes")
	private static Map<String, Class<? extends ContentSerializer>> scanContentSerializers() {
		Map<String, Class<? extends ContentSerializer>> map = new HashMap<String, Class<? extends ContentSerializer>>();
		for (Class<? extends ContentSerializer> serClass : Reflections.getSubClasses(ContentSerializer.class)) {
			if (Modifier.isAbstract(serClass.getModifiers())) continue;
			ContentSerializer<?> def = Instances.fetch(serClass, DEFAULT_CONTENT_TYPE.getMimeType());
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

	private static final Set<String> NO_CLASS_SER = new HashSet<>(Arrays.asList("net.butfly.albacore.serialize.HessianSerializer",
			"net.butfly.albacore.serialize.JSONSerializer", "net.butfly.albacore.serialize.BurlapSerializer"));

	// XXX: check the serializer need class info.
	public static boolean isSupportClass(Class<? extends Serializer<?>> ser) {
		return NO_CLASS_SER.contains(ser.getName());
	}
}
