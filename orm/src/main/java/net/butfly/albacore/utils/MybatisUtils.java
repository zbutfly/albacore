package net.butfly.albacore.utils;

import java.util.Map;

import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;

public class MybatisUtils extends UtilsBase {
	private static final ObjectFactory DEFAULT_OBJECT_FACTORY = new DefaultObjectFactory();
	private static final ObjectWrapperFactory DEFAULT_OBJECT_WRAPPER_FACTORY = new DefaultObjectWrapperFactory();

	public static final MetaObject createMeta(Object target) {
		MetaObject meta = MetaObject.forObject(target, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY);
		while (meta.hasGetter("target") || meta.hasGetter("h")) {
			while (meta.hasGetter("h")) {
				Object object = meta.getValue("h");
				meta = MetaObject.forObject(object, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY);
			}
			while (meta.hasGetter("target")) {
				Object object = meta.getValue("target");
				meta = MetaObject.forObject(object, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY);
			}
		}
		return meta;
	}

	public static Map<? extends String, ? extends Object> convertToMap(MetaObject meta) {
		// TODO Auto-generated method stub
		return null;
	}
}
