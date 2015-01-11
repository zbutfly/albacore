/*
 * Copyright 2012 MyBatis.org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.butfly.albacore.utils.imports.meta;

import net.butfly.albacore.utils.imports.meta.factory.DefaultObjectFactory;
import net.butfly.albacore.utils.imports.meta.factory.ObjectFactory;
import net.butfly.albacore.utils.imports.meta.wrapper.DefaultObjectWrapperFactory;
import net.butfly.albacore.utils.imports.meta.wrapper.ObjectWrapperFactory;

/**
 * @author Clinton Begin
 */
public class SystemMetaObject {

	public static final ObjectFactory DEFAULT_OBJECT_FACTORY = new DefaultObjectFactory();
	public static final ObjectWrapperFactory DEFAULT_OBJECT_WRAPPER_FACTORY = new DefaultObjectWrapperFactory();
	public static final MetaObject NULL_META_OBJECT = MetaObject.forObject(NullObject.class, DEFAULT_OBJECT_FACTORY,
			DEFAULT_OBJECT_WRAPPER_FACTORY);

	private static class NullObject {}

	public static MetaObject forObject(Object object) {
		return MetaObject.forObject(object, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY);
	}

}
