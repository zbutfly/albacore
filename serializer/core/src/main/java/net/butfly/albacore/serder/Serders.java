package net.butfly.albacore.serder;

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

@SuppressWarnings("rawtypes")
public final class Serders extends Utils {
	public static final ContentType DEFAULT_CONTENT_TYPE = ContentType.WILDCARD.withCharset(Charsets.UTF_8);
	public static final Class<? extends Serder<?, ?>> DEFAULT_SERIALIZER_CLASS = scanDefaultSerializer();

	@SuppressWarnings("unchecked")
	public static Class<? extends ContentSerder<?, ?>> contentSerializers(String mimeType) {
		return (Class<? extends ContentSerder<?, ?>>) Instances.fetch(Serders::scanContentSerializers).get(mimeType);
	}

	private static Map<String, Class<? extends ContentSerder>> scanContentSerializers() {
		Map<String, Class<? extends ContentSerder>> map = new HashMap<String, Class<? extends ContentSerder>>();
		for (Class<? extends ContentSerder> serClass : Reflections.getSubClasses(ContentSerder.class)) {
			if (Modifier.isAbstract(serClass.getModifiers())) continue;
			ContentSerder<?, ?> def = Instances.fetch(serClass, DEFAULT_CONTENT_TYPE.getMimeType());
			for (String mime : def.supportedMimeTypes())
				map.put(mime, serClass);
		}
		return map;

	}

	public static Serder<?, ?> serializer(final Class<? extends Serder<?, ?>> serializerClass, final Charset charset) {
		return Instances.fetch(serializerClass, charset);
	}

	private static final String[] INTERN_SER = new String[] { "net.butfly.albacore.serialize.HessianSerializer",
			"net.butfly.albacore.serialize.JSONSerializer" };

	@SuppressWarnings("unchecked")
	private static Class<? extends Serder<?, ?>> scanDefaultSerializer() {
		for (Class<? extends Serder> cl : Reflections.getSubClasses(Serder.class))
			if (!Modifier.isAbstract(cl.getModifiers()) && !cl.getName().startsWith("net.butfly.albacore.serialize."))
				return (Class<? extends Serder<?, ?>>) cl;
		for (String s : INTERN_SER) {
			Class<? extends Serder<?, ?>> cl = Reflections.forClassName(s);
			if (null != cl) return cl;
		}
		throw new RuntimeException("Could not found any serializer class implementation in classpath.");
	}

	private static final Set<String> NO_CLASS_SER = new HashSet<>(Arrays.asList("net.butfly.albacore.serialize.HessianSerializer",
			"net.butfly.albacore.serialize.JSONSerializer", "net.butfly.albacore.serialize.BurlapSerializer"));

	// XXX: check the serializer need class info.
	public static boolean isSupportClass(Class<? extends Serder> ser) {
		return NO_CLASS_SER.contains(ser.getName());
	}
}
