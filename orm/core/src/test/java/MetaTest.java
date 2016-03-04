import java.util.Arrays;
import java.util.Map;

import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.utils.Objects;
import net.butfly.albacore.utils.imports.meta.MetaObject;

public class MetaTest {
	static class A {
		String a;
	}

	static class B {
		int b;
		A a;
	}

	static class BB extends B {
		String bb;
	}

	public static void main(String[] args) {
		Page page = new Page(15, 1);
		Map<String, Object> map = Objects.toMap(page);
		for (String k : map.keySet()) {
			System.out.println(k + ":" + map.get(k).getClass().getName());
		}

		double d = 12345678.345;
		assert (Double.class.cast(d) > 0);
		assert ((byte) d > 0);
		printMetaInfo(true);
		printMetaInfo("sdfsfsdfdsf");
		printMetaInfo(new Byte((byte) 12));
		A a = new A();
		a.a = "stringA";
		printMetaInfo(a);
		B b = new B();
		b.b = 123456;
		b.a = a;
		printMetaInfo(b);
		BB bb = new BB();
		bb.b = 123456;
		bb.a = a;
		bb.bb = "stringBB";
		printMetaInfo(bb);
		BB[] bbs = new BB[] { bb };
		printMetaInfo(Arrays.asList(bbs));
		MetaObject meta = Objects.createMeta(bbs);
		System.out.println(meta.getValue("a.a"));
		System.out.println(meta.findProperty("a.a", true));
	}

	private static void printMetaInfo(Object target) {
		System.out.println(target.getClass().getName());
		MetaObject meta = Objects.createMeta(target);
		for (String name : meta.getGetterNames())
			System.out.println("<==" + "get [" + name + "]: " + meta.getValue(name) + "[" + meta.getGetterType(name).getName() + "]");
		for (String name : meta.getSetterNames())
			System.out.println("==>" + "set [" + name + "]: " + "[" + meta.getGetterType(name).getName() + "]");
		System.out.println("=================================");
	}
}
