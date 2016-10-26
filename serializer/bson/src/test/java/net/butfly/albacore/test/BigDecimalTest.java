package net.butfly.albacore.test;

import java.io.IOException;
import java.math.BigDecimal;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class BigDecimalTest {
	public static void main(String... args) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper()//
				.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)//
				.configure(JsonParser.Feature.IGNORE_UNDEFINED, true)//
				.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)//
				.enable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)//
				.enable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)//
				.enable(SerializationFeature.WRITE_ENUMS_USING_INDEX)//
				.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)//
		;
		BigDecimal bg = mapper.readValue("40.", BigDecimal.class);
		System.out.println(bg);
	}
}
