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
package net.butfly.albacore.utils.imports.meta.wrapper;

import java.util.Collection;
import java.util.List;

import net.butfly.albacore.utils.imports.meta.MetaObject;
import net.butfly.albacore.utils.imports.meta.factory.ObjectFactory;
import net.butfly.albacore.utils.imports.meta.property.PropertyTokenizer;

public class CollectionWrapper implements ObjectWrapper {
	private final Collection<Object> object;

	public CollectionWrapper(final MetaObject metaObject, final Collection<Object> object) {
		this.object = object;
	}

	@Override
	public Object get(final PropertyTokenizer prop) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void set(final PropertyTokenizer prop, final Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String findProperty(final String name, final boolean useCamelCaseMapping) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String[] getGetterNames() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String[] getSetterNames() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Class<?> getSetterType(final String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Class<?> getGetterType(final String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasSetter(final String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasGetter(final String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaObject instantiatePropertyValue(final String name, final PropertyTokenizer prop, final ObjectFactory objectFactory) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isCollection() {
		return true;
	}

	@Override
	public void add(final Object element) {
		object.add(element);
	}

	@Override
	public <E> void addAll(final List<E> element) {
		object.addAll(element);
	}
}
