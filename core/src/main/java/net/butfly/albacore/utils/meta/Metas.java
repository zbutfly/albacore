package net.butfly.albacore.utils.meta;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.List;

import com.google.common.base.CaseFormat;
import com.google.common.base.Splitter;

import net.butfly.albacore.utils.Utils;

public final class Metas extends Utils {
	public static String fieldToColumn(String fieldName) {
		return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, fieldName);
	}

	public static String[] propToFields(String propName) {
		List<String> props = Splitter.on(".").splitToList(propName);
		for (int i = 0; i < props.size(); i++)
			props.set(i, CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, props.get(i)));
		return props.toArray(new String[props.size()]);
	}

	public static String getProps(final Class<?> clazz) {
		try {
			BeanInfo info = Introspector.getBeanInfo(clazz);
			PropertyDescriptor[] props = info.getPropertyDescriptors();
			for (PropertyDescriptor pd : props) {
				return pd.getName();
			}
		} catch (IntrospectionException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}
}
