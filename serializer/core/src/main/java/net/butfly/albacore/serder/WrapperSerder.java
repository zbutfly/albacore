package net.butfly.albacore.serder;

import com.google.common.reflect.TypeToken;

class WrapperSerder<PRESENT, DATA, RESULT> implements Serder<PRESENT, RESULT> {
	private static final long serialVersionUID = 167713381996507955L;
	protected Serder<PRESENT, DATA> first;
	protected Serder<DATA, RESULT> second;
	protected TypeToken<DATA> data;

	public WrapperSerder(Serder<PRESENT, DATA> first, Serder<DATA, RESULT> second, TypeToken<DATA> data) {
		super();
		this.first = first;
		this.second = second;
		this.data = data;
	}

	@Override
	public <T extends PRESENT> RESULT ser(T from) {
		return second.ser(first.ser(from));
	}

	@Override
	public <T extends PRESENT> T der(RESULT from, TypeToken<T> to) {
		return first.der(second.der(from, data), to);
	}
}
