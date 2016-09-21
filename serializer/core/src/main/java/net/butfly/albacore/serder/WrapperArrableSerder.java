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

	@SafeVarargs
	@Override
	public final PRESENT[] der(RESULT from, TypeToken<? extends PRESENT>... tos) {
		List<TypeToken<DATA>> ds = new ArrayList<>();
		for (int i = 0; i < tos.length; i++)
			ds.add(data);
		DATA[] d = ((ArrableSerder<DATA, RESULT>) second).der(from, ds.toArray(Generics.array()));
		if (null == d) return null;
		List<PRESENT> results = new ArrayList<>();
		for (int i = 0; i < d.length; i++)
			results.add(first.der(d[i], tos[i]));
		return results.toArray(Generics.array());
	}
}
