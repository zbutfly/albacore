package net.butfly.albacore.utils;

public abstract class Utils/* <U extends Utils<U>> */ {
	// protected final Logger logger = Instances.fetch(new
	// Runnable.Callable<Logger>() {
	// @Override
	// public Logger call() {
	// return Logger.getLogger(Utils.this.getClass());
	// }
	// }, getClass());

	protected Utils() {}

	// protected static final <U extends Utils<U>> U instance(final Class<U>
	// utilsClass) {
	// return Instances.fetch(new Runnable.Callable<U>() {
	// @SuppressWarnings("unchecked")
	// @Override
	// public U create() {
	// try {
	// return (U) Generics.getGenericParamClass(utilsClass, Utils.class,
	// "U").newInstance();
	// } catch (Exception e) {
	// return null;
	// }
	// }
	// }, utilsClass);
	// }
}
