package net.butfly.albacore.meta;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.MethodDescriptor;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;

import net.butfly.albacore.utils.Generics;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Logger;

public class MetaTest {
	private static final Logger logger = Logger.getLogger(MetaTest.class);

	static class A<T> {
		public int i;
		private String s;
		private T t;
		private double[] ds;

		public String getS() {
			return s;
		}

		public void setS(String s) {
			this.s = s;
		}

		public double[] getDs() {
			return ds;
		}

		public void setDs(double[] ds) {
			this.ds = ds;
		}

		public double getDs(int i) {
			return ds[i];
		}

		public void setDs(int i, double ds) {
			this.ds[i] = ds;
		}

		public T getT() {
			return t;
		}

		public void setT(T t) {
			this.t = t;
		}
	}

	static class B extends A<String> {
		public String name;
		private boolean gender;
		private A<B> b;

		public boolean isGender() {
			return gender;
		}

		public void setGender(boolean gender) {
			this.gender = gender;
		}

		public A<B> getB() {
			return b;
		}

		public void setB(A<B> b) {
			this.b = b;
		}
	}

	public static void main(String[] args) throws IntrospectionException, InstantiationException, IllegalAccessException,
			NoSuchFieldException, SecurityException {
		BeanInfo info = Introspector.getBeanInfo(B.class);
		for (PropertyDescriptor p : info.getPropertyDescriptors())
			logger.info(() -> p.toString());

		for (MethodDescriptor m : info.getMethodDescriptors())
			logger.info(() -> m.toString());

		logger.info(() -> Generics.resolveGenericParameter(B.class, A.class, "T").toString());
		logger.info(() -> Generics.resolveFieldType(B.class, Reflections.getDeclaredField(B.class, "t")).toString());
		for (Method m : Reflections.getDeclaredMethods(B.class))
			if ("getT".equals(m.getName())) logger.info(() -> "getT return: " + Generics.resolveReturnType(B.class, m));

	}
}
