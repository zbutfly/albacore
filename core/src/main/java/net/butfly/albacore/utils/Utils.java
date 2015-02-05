package net.butfly.albacore.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Utils/*<U extends Utils<U>>*/ {
	protected final Logger logger = Instances.fetch(new Instances.Instantiator<Logger>() {
		@Override
		public Logger create() {
			return LoggerFactory.getLogger(Utils.this.getClass());
		}
	}, getClass());

	protected Utils() {}

//	protected static final <U extends Utils<U>> U instance(final Class<U> utilsClass) {
//		return Instances.fetch(new Instances.Instantiator<U>() {
//			@SuppressWarnings("unchecked")
//			@Override
//			public U create() {
//				try {
//					return (U) Generics.getGenericParamClass(utilsClass, Utils.class, "U").newInstance();
//				} catch (Exception e) {
//					return null;
//				}
//			}
//		}, utilsClass);
//	}
}
