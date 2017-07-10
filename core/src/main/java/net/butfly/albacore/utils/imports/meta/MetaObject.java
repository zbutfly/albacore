package net.butfly.albacore.utils.imports.meta;

/*
 *    Copyright 2009-2012 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.imports.meta.factory.ObjectFactory;
import net.butfly.albacore.utils.imports.meta.property.PropertyTokenizer;
import net.butfly.albacore.utils.imports.meta.wrapper.BeanWrapper;
import net.butfly.albacore.utils.imports.meta.wrapper.CollectionWrapper;
import net.butfly.albacore.utils.imports.meta.wrapper.MapWrapper;
import net.butfly.albacore.utils.imports.meta.wrapper.ObjectWrapper;
import net.butfly.albacore.utils.imports.meta.wrapper.ObjectWrapperFactory;

public class MetaObject {
	private Object originalObject;
	private ObjectWrapper objectWrapper;
	private ObjectFactory objectFactory;
	private ObjectWrapperFactory objectWrapperFactory;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private MetaObject(Object object, ObjectFactory objectFactory, ObjectWrapperFactory objectWrapperFactory) {
		this.originalObject = object;
		this.objectFactory = objectFactory;
		this.objectWrapperFactory = objectWrapperFactory;

		if (object instanceof ObjectWrapper) {
			this.objectWrapper = (ObjectWrapper) object;
		} else if (objectWrapperFactory.hasWrapperFor(object)) {
			this.objectWrapper = objectWrapperFactory.getWrapperFor(this, object);
		} else if (object instanceof Map) {
			this.objectWrapper = new MapWrapper(this, (Map) object);
		} else if (object instanceof Collection) {
			this.objectWrapper = new CollectionWrapper(this, (Collection) object);
		} else {
			this.objectWrapper = new BeanWrapper(this, object);
		}
	}

	public static MetaObject forObject(Object object, ObjectFactory objectFactory, ObjectWrapperFactory objectWrapperFactory) {
		if (object == null) {
			return SystemMetaObject.NULL_META_OBJECT;
		} else {
			return new MetaObject(object, objectFactory, objectWrapperFactory);
		}
	}

	public boolean isMetaNull() {
		return equals(SystemMetaObject.NULL_META_OBJECT);
	}

	public Class<?> getOriginalClass() {
		return isMetaNull() ? null : originalObject.getClass();
	}

	public ObjectFactory getObjectFactory() {
		return objectFactory;
	}

	public ObjectWrapperFactory getObjectWrapperFactory() {
		return objectWrapperFactory;
	}

	public Object getOriginalObject() {
		return originalObject;
	}

	public String findProperty(String propName, boolean useCamelCaseMapping) {
		return objectWrapper.findProperty(propName, useCamelCaseMapping);
	}

	public String[] getGetterNames() {
		return objectWrapper.getGetterNames();
	}

	public String[] getSetterNames() {
		return objectWrapper.getSetterNames();
	}

	public Class<?> getSetterType(String name) {
		return objectWrapper.getSetterType(name);
	}

	public Class<?> getGetterType(String name) {
		return objectWrapper.getGetterType(name);
	}

	public boolean hasSetter(String name) {
		return objectWrapper.hasSetter(name);
	}

	public boolean hasGetter(String name) {
		return objectWrapper.hasGetter(name);
	}

	public Object getValue(String name) {
		PropertyTokenizer prop = new PropertyTokenizer(name);
		if (prop.hasNext()) {
			MetaObject metaValue = metaObjectForProperty(prop.getIndexedName());
			if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
				return null;
			} else {
				return metaValue.getValue(prop.getChildren());
			}
		} else {
			return objectWrapper.get(prop);
		}
	}

	public void setValue(String name, Object value) {
		PropertyTokenizer prop = new PropertyTokenizer(name);
		if (prop.hasNext()) {
			MetaObject metaValue = metaObjectForProperty(prop.getIndexedName());
			if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
				if (value == null && prop.getChildren() != null) {
					return; // don't instantiate child path if value is null
				} else {
					metaValue = objectWrapper.instantiatePropertyValue(name, prop, objectFactory);
				}
			}
			metaValue.setValue(prop.getChildren(), value);
		} else {
			objectWrapper.set(prop, value);
		}
	}

	public MetaObject metaObjectForProperty(String name) {
		Object value = getValue(name);
		return MetaObject.forObject(value, objectFactory, objectWrapperFactory);
	}

	public ObjectWrapper getObjectWrapper() {
		return objectWrapper;
	}

	public boolean isCollection() {
		return objectWrapper.isCollection();
	}

	public void add(Object element) {
		objectWrapper.add(element);
	}

	public <E> void addAll(List<E> list) {
		objectWrapper.addAll(list);
	}

}
