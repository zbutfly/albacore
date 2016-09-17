package net.butfly.bus.test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JacksonTest {
	public enum Enums {
		V1, V2, V3
	}

	public static class Bean implements Serializable {
		private static final long serialVersionUID = -2963162163893587423L;
		public int ivalue;
		public long lvalue;
		public Enums evalue;
		public String svalue;
		public Bean bvalue;

		public Bean() {
			super();
			this.ivalue = (int) (Math.random() * 10);
			this.lvalue = (long) (Math.random() * 10);
			this.svalue = "asdasd";
			this.evalue = Enums.values()[(int) (Math.random() * 3)];
		}
	}

	public static void main(String[] args) throws IOException {
		Bean[] beans = new Bean[] { new Bean(), new Bean(), new Bean() };
		beans[2].bvalue = beans[1];

		ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
				.enable(SerializationFeature.WRITE_ENUMS_USING_INDEX)
				.enable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)
				.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
				.enable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
				.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);

		String json = mapper.writeValueAsString(Arrays.asList(beans));
		System.out.println(json);
		List<Bean> rl = mapper.readValue(json, new TypeReference<List<Bean>>() {});
		assert (rl.size() == 3);
		Bean[] ra = mapper.readValue(json, Bean[].class);
		assert (ra.length == 3);
		JsonNode tree = mapper.readTree(json);
		assert (tree.isArray());
	}
}
