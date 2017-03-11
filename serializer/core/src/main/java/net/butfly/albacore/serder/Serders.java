package net.butfly.albacore.serder;

import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.entity.ContentType;

import com.google.common.base.Charsets;

import net.butfly.albacore.serder.support.ClassInfoSerder;
import net.butfly.albacore.serder.support.ContentTypeSerder;
import net.butfly.albacore.serder.support.ContentTypes;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.Utils;

@SuppressWarnings("rawtypes")
public final class Serders extends Utils {
	static final Pair<CaseFormat, CaseFormat> DEFAULT_MAPPING = new Pair<>(CaseFormat.LOWER_CAMEL, CaseFormat.UPPER_UNDERSCORE);
	public static final CaseFormat DEFAULT_SRC_FORMAT = CaseFormat.LOWER_CAMEL;
	public static final CaseFormat DEFAULT_DST_FORMAT = CaseFormat.UPPER_UNDERSCORE;
	public static final ContentType DEFAULT_CONTENT_TYPE = ContentType.WILDCARD.withCharset(Charsets.UTF_8);

	private static final String[] INTERN_SER = new String[] { //
			"net.butfly.albacore.serder.HessianSerder", //
			"net.butfly.albacore.serder.JSONSerializer", //
			JavaSerder.class.getName() };
	public static final Class<? extends Serder<?, ?>> DEFAULT_SERIALIZER_CLASS = getSerderByDefault();

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

	private static final Map<String, ContentTypeSerder> SERDERS = new ConcurrentHashMap<>();

	public static ContentTypeSerder construct(ContentType contentType) {
		return SERDERS.computeIfAbsent(contentType.getMimeType() + "," + contentType.getCharset().toString(), ct -> {
			for (Class<? extends ContentTypeSerder> c : Reflections.getSubClasses(ContentTypeSerder.class)) {
				if (Modifier.isAbstract(c.getModifiers()) || Modifier.isStatic(c.getModifiers())) continue;
				ContentTypeSerder s = Reflections.construct(c);
				if (ContentTypes.mimeMatch(s.contentType().getMimeType(), contentType.getMimeType())) {
					s.charset(contentType.getCharset());
					return s;
				}
			}
			throw new RuntimeException("Serder not found for content type: " + contentType.toString());
		});
	}

	public static Serder<?, ?> serializer(final Class<? extends Serder<?, ?>> serializerClass, final Charset charset) {
		return Instances.fetch(serializerClass);
	}

	public static boolean isClassInfoSupported(Class<? extends Serder> ser) {
		return ClassInfoSerder.class.isAssignableFrom(ser);
	}
}
