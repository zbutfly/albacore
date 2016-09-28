package net.butfly.albacore.serder.support.converter;

@SuppressWarnings("rawtypes")
public class EnumConverter implements Converter<Enum, Integer> {
	@Override
	public Integer serialize(Enum original) {
		return null == original ? null : original.ordinal();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <S extends Enum> S deserialize(Integer replaced) {
		if (null == replaced) return null;
		else try {
			return ((S[]) this.getOriginalClass().getMethod("values").invoke(null))[replaced];
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public Class<Enum> getOriginalClass() {
		return Enum.class;
	}

	// @Override
	// public Class<Integer> getReplacedClass() {
	// return Integer.class;
	// }
}
