package net.butfly.albacore.serder;

import java.lang.reflect.Field;
import java.util.function.Function;

import net.butfly.albacore.serder.modifier.Property;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Reflections;

public abstract class SerderBase<PRESENT, DATA> implements Serder<PRESENT, DATA> {
	private static final long serialVersionUID = 5308575743954634527L;
	private static final CaseFormat DEFAULT_SRC_FORMAT = CaseFormat.LOWER_CAMEL;
	private static final CaseFormat DEFAULT_DST_FORMAT = CaseFormat.UPPER_UNDERSCORE;
	private final Function<String, String> mapping;

	public SerderBase() {
		this(s -> DEFAULT_SRC_FORMAT.to(DEFAULT_DST_FORMAT, s));
	}

	public SerderBase(CaseFormat dstFormat) {
		this(s -> DEFAULT_SRC_FORMAT.to(dstFormat, s));
	}

	public SerderBase(CaseFormat srcFormat, CaseFormat dstFormat) {
		this(s -> srcFormat.to(dstFormat, s));
	}

	public SerderBase(Function<String, String> mapping) {
		super();
		this.mapping = mapping;
	}

	@Override
	public String parseQualifier(Field f) {
		Reflections.noneNull("", f);
		return f.isAnnotationPresent(Property.class) ? f.getAnnotation(Property.class).value() : mapping.apply(f.getName());
	}
}
