package net.butfly.albacore.serder;

public interface WrapSerder<PRESENT, MEDIUM, DATA, S1 extends Serder<PRESENT, MEDIUM>, S2 extends Serder<MEDIUM, DATA>> extends
		Serder<PRESENT, DATA> {
	S1 s1();

	S2 s2();

	Class<MEDIUM> medium();

	@Override
	default DATA serialize(Object from) {
		return s2().serializeT(s1().serialize(from));
	}

	@Override
	default Object deserialize(DATA from, Class<?> to) {
		return deserializeT(from, to);
	}

	@Override
	default <T> T deserializeT(DATA from, Class<T> to) {
		return s1().deserializeT(s2().deserializeT(from, medium()), to);
	}
}
