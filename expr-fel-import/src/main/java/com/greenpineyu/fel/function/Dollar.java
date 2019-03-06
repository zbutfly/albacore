package com.greenpineyu.fel.function;

import java.lang.reflect.InvocationTargetException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.greenpineyu.fel.FelEngine;
import com.greenpineyu.fel.common.StringUtils;
import com.greenpineyu.fel.compile.FelMethod;
import com.greenpineyu.fel.compile.SourceBuilder;
import com.greenpineyu.fel.context.FelContext;
import com.greenpineyu.fel.parser.FelNode;

/**
 * $函数，通过$获取class或者创建对象 ${Math} 结果为 Math.class ${Dollar.new} 结果为 new Dollar()
 * 
 * @author yuqingsong
 * 
 */
public class Dollar implements Function {
	private static final Logger logger = LoggerFactory.getLogger(Function.class);

	@Override
	public String getName() {
		return "$";
	}

	@Override
	public Object call(FelNode node, FelContext context) {
		String txt = getChildText(node);

		boolean isNew = isNew(txt);
		Class<?> cls = getClass(txt, isNew);
		if (isNew) {
			return newObject(cls);

		} else {
			return cls;
		}
	}

	private Object newObject(Class<?> cls) {
		Object o = null;
		if (cls != null) {
			try {
				o = cls.getConstructor().newInstance();
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e) {}
		}
		return o;
	}

	private static final String suffix = ".new";

	@SuppressWarnings("unused")
	private Class<?> getClass(FelNode node) {
		String txt = getChildText(node);
		boolean isNew = isNew(txt);
		return getClass(txt, isNew);
	}

	private Class<?> getClass(String txt, boolean isNew) {
		String className = txt;
		if (isNew) {
			className = className.substring(0, txt.length() - suffix.length());
		}
		if (className.indexOf(".") == -1) {
			className = "java.lang." + className;
		}
		try {
			Class<?> clz = Class.forName(className);
			return clz;
		} catch (ClassNotFoundException e) {
			logger.error("", e);
		}
		return null;
	}

	private boolean isNew(String txt) {
		boolean isNew = txt.endsWith(suffix);
		return isNew;
	}

	private String getChildText(FelNode node) {
		String txt = node.getChildren().get(0).getText();
		txt = StringUtils.remove(txt, '\'');
		txt = StringUtils.remove(txt, '"');
		return txt;
	}

	@Override
	public SourceBuilder toMethod(FelNode node, FelContext ctx) {
		String txt = getChildText(node);

		boolean isNew = isNew(txt);
		Class<?> cls = getClass(txt, isNew);
		String code = cls.getName();
		if (isNew) {
			code = "new " + code + "()";
		}
		return new FelMethod(cls, code);
	}

	public static void main(String[] args) {
		// System.out.println("abc.new".endsWith(".new"));
		String exp = "$('Math').max($('Math').min(1,2),3).doubleValue()";
		exp = "$('String.new').concat('abc')";
		Object eval = FelEngine.instance.eval(exp);
		System.out.println(eval);
	}
}
