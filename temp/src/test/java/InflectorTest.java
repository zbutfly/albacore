import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.ExtendedBeanInfoFactory;

import net.butfly.abalbacore.utils.lang.Objects;

public class InflectorTest {
	public int field;

	// to prove getMethod distincts primitive class from its boxed class.
	public long setOne(long value) {
		return value;
	}

	// to prove conflict get/set, get is preferential
	public void setAnother(double value) {}

	public CharSequence getAnother() {
		return "";
	}

	class MoreInflectorTest extends InflectorTest {
		public InflectorTest[] getMore() {
			return null;
		}
	}

	public static void main(String[] args) throws NoSuchMethodException, SecurityException, IntrospectionException {
		BeanInfo info1 = Introspector.getBeanInfo(MoreInflectorTest.class);
		BeanInfo info2 = new ExtendedBeanInfoFactory().getBeanInfo(MoreInflectorTest.class);
		for (PropertyDescriptor pd : info1.getPropertyDescriptors())
			System.out.println(pd.getName());
		for (PropertyDescriptor pd : info2.getPropertyDescriptors())
			System.out.println(pd.getName());
		// testMappize();
	}

	static void testMappize() {
		ClassA[] aArray = new ClassA[12];
		for (int i = 0; i < aArray.length; i++)
			aArray[i] = new ClassA();
		Map<String, ClassB> bMap = new HashMap<String, ClassB>();
		for (int i = 0; i < 7; i++)
			bMap.put("key" + i, new ClassB());
		ClassC cObject = new ClassC();

		System.out.println(Objects.flatten(12345));
		System.out.println(Objects.flatten(aArray));
		System.out.println(Objects.flatten(bMap));
		System.out.println(Objects.flatten(cObject));

	}

	static class ClassA {
		String aName = "aaa";
		int intValue = (int) (Math.random() * 100);
		Float floatValue = (float) (Math.random() * 100);
	}

	static class ClassB {
		String bName = "bbb";
		byte[] bytes = new byte[(int) (Math.random() * 10)];
		Map<String, Long> longMap = new HashMap<String, Long>();
		ClassA aObject = new ClassA();

		ClassB() {
			Arrays.fill(bytes, (byte) (Math.random() * 100));
			for (int i = 0; i < 5; i++)
				longMap.put("key" + i, (long) (Math.random() * 1000));
		}
	}

	static class ClassC {
		String cName = "ccc";
		Long longValue = (long) (Math.random() * 1000);
		ClassB bObject = new ClassB();
		List<ClassA> aList = new ArrayList<ClassA>();

		ClassC() {
			for (int i = 0; i < (int) (Math.random() * 10); i++)
				aList.add(new ClassA());
		}
	}
}
