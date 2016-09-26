package net.butfly.albacore.serder;

import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.entity.ContentType;

import com.google.common.base.Charsets;

import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.Utils;
import scala.Tuple2;

@SuppressWarnings("rawtypes")
public final class Serders extends Utils {
	static final Tuple2<CaseFormat, CaseFormat> DEFAULT_MAPPING = new Tuple2<>(CaseFormat.LOWER_CAMEL, CaseFormat.UPPER_UNDERSCORE);
	public static final CaseFormat DEFAULT_SRC_FORMAT = CaseFormat.LOWER_CAMEL;
	public static final CaseFormat DEFAULT_DST_FORMAT = CaseFormat.UPPER_UNDERSCORE;
	public static final ContentType DEFAULT_CONTENT_TYPE = ContentType.WILDCARD.withCharset(Charsets.UTF_8);
	public static final Class<? extends Serder<?, ?>> DEFAULT_SERIALIZER_CLASS = getSerderByDefault();

	private static final String[] INTERN_SER = new String[] { //
			"net.butfly.albacore.serder.HessianSerder", //
			"net.butfly.albacore.serder.JSONSerializer", //
			JavaSerder.class.getName() };

	@SuppressWarnings("unchecked")
	private static Class<? extends Serder<?, ?>> getSerderByDefault() {
		for (Class<? extends Serder> cl : Reflections.getSubClasses(Serder.class))
			if (!Modifier.isAbstract(cl.getModifiers()) && !cl.getName().startsWith("net.butfly.albacore.serder."))
				return (Class<? extends Serder<?, ?>>) cl;
		for (String s : INTERN_SER) {
			Class<? extends Serder<?, ?>> cl = Reflections.forClassName(s);
			if (null != cl) return cl;
		}
		return JavaSerder.class;
	}

	@SuppressWarnings("unchecked")
	public static Class<? extends ContentSerder<?, ?>> getSerderByMimeType(String mimeType) {
		return (Class<? extends ContentSerder<?, ?>>) Instances.fetch(Serders::scanContentSerders, Map.class).get(mimeType);
	}

	private static Map<String, Class<? extends ContentSerder>> scanContentSerders() {
		Map<String, Class<? extends ContentSerder>> map = new HashMap<String, Class<? extends ContentSerder>>();
		for (Class<? extends ContentSerder> serClass : Reflections.getSubClasses(ContentSerder.class)) {
			if (Modifier.isAbstract(serClass.getModifiers())) continue;
			ContentSerder<?, ?> def = Instances.fetch(serClass, DEFAULT_CONTENT_TYPE.getMimeType());
			for (String mime : def.mimeTypes())
				map.put(mime, serClass);
		}
		return map;

	}

	public static Serder<?, ?> serializer(final Class<? extends Serder<?, ?>> serializerClass, final Charset charset) {
		return Instances.fetch(serializerClass, charset);
	}

	@Deprecated
	public static boolean isClassInfoSupported(Class<? extends Serder> ser) {
		return ClassInfoSerder.class.isAssignableFrom(ser);
	}
}
