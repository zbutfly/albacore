package net.butfly.albacore.serder;

import java.io.Serializable;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Objects;

public interface Serder<PRESENT, DATA> extends Serializable {
	DATA ser(PRESENT from);

	<T extends PRESENT> T der(DATA from, Class<T> to);

	default Object[] der(DATA from, Class<?>... tos) {
		throw new UnsupportedOperationException();
	}

	default Converter<PRESENT, DATA> converter() {
		return this::ser;
	}

	default <T extends PRESENT> Converter<DATA, T> unconverter(Class<T> present) {
		return d -> der(d, present);
	}

	default String mappingFieldName(String fieldName) {
		return fieldName;
	}

	default String unmappingFieldName(Class<?> obj, String propName) {
		return propName;
	}

	default <RESULT> Serder<PRESENT, RESULT> then(Serder<DATA, RESULT> next, Class<DATA> dataClass) {
		return new Serder<PRESENT, RESULT>() {
			private static final long serialVersionUID = -1235704170589079845L;

			@Override
			public RESULT ser(PRESENT from) {
				return next.ser(Serder.this.ser(from));
			}

			@Override
			public <T extends PRESENT> T der(RESULT from, Class<T> to) {
				return Serder.this.der(next.der(from, dataClass), to);
			}
		};
	}

	default Serder<PRESENT, DATA> mapping(CaseFormat from, CaseFormat to) {
		Objects.noneNull(from, to);
		return new Serder<PRESENT, DATA>() {
			private static final long serialVersionUID = -3950077991740462528L;

			@Override
			public DATA ser(PRESENT from) {
				return Serder.this.ser(from);
			}

			@Override
			public <T extends PRESENT> T der(DATA from, Class<T> to) {
				return Serder.this.der(from, to);
			}

			@Override
			public Object[] der(DATA from, Class<?>... tos) {
				return Serder.this.der(from, tos);
			}

			@Override
			public String mappingFieldName(String fieldName) {
				return from.to(to, fieldName);
			}

			@Override
			public String unmappingFieldName(Class<?> data, String propName) {
				return to.to(from, propName);
			}
		};
	}
}