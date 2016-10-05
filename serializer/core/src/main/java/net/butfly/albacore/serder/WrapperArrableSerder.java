package net.butfly.albacore.serder;

import java.util.ArrayList;
import java.util.List;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.utils.Generics;

class WrapperArrableSerder<PRESENT, DATA, RESULT> extends WrapperSerder<PRESENT, DATA, RESULT> implements ArrableSerder<PRESENT, RESULT> {
	private static final long serialVersionUID = 167713381996507955L;

	public WrapperArrableSerder(Serder<PRESENT, DATA> first, ArrableSerder<DATA, RESULT> second, TypeToken<DATA> data) {
		super(first, second, data);
	}

	@SuppressWarnings({ "unchecked", "serial" })
	@SafeVarargs
	@Override
	public final Object[] der(RESULT from, Class<?>... tos) {
		List<TypeToken<DATA>> ds = new ArrayList<>();
		for (int i = 0; i < tos.length; i++)
			ds.add(data);
		Object[] d = ((ArrableSerder<DATA, RESULT>) second).der(from, ds.toArray(Generics.array())); // DATA[]

		if (null == d) return null;
		List<PRESENT> results = new ArrayList<>();
		for (int i = 0; i < d.length; i++)
			results.add(first.der((DATA) d[i], new TypeToken<PRESENT>(tos[i]) {}));
		return results.toArray(Generics.array());
	}
}
