package net.butfly.albacore.dbo.criteria;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.support.ObjectSupport;

public class Criteria extends ObjectSupport<Criteria> {
	private static final long serialVersionUID = 4775216639071589206L;
	@Deprecated
	private static final String SCHEMA_PARAM_NAME = "schema";
	private static final String ORDER_FIELDS_PARAM_NAME = "orderFields";
	protected Map<String, Object> params;
	protected List<OrderField> orderFields;

	public List<OrderField> getOrderFields() {
		return orderFields;
	}

	public Criteria() {
		this.params = new HashMap<String, Object>();
		this.orderFields = new ArrayList<Criteria.OrderField>();
	};

	@Deprecated
	public Criteria(String schema) {
		this();
		this.params.put(SCHEMA_PARAM_NAME, schema);
	}

	public Criteria addOrder(String orderField) {
		return this.addOrder(orderField, true);
	}

	public Criteria addOrder(String orderField, boolean asc) {
		this.orderFields.add(new OrderField(orderField, asc));
		return this;
	}

	public Criteria setParameters(Map<String, ?> params) {
		this.params.putAll(params);
		return this;
	}

	public Criteria set(String key, Object value) {
		this.params.put(key, value);
		return this;
	}

	public Map<String, Object> getParameters() {
		return this.getParameters(false);
	}

	public Map<String, Object> getParameters(boolean pure) {
		Map<String, Object> p = new HashMap<String, Object>(this.params);
		if (!pure)
			if (this.orderFields.size() > 0) p.put(ORDER_FIELDS_PARAM_NAME,
					this.orderFields.toArray(new OrderField[this.orderFields.size()]));
			else p.remove(ORDER_FIELDS_PARAM_NAME);
		return p;
	}

	public static final class OrderField {
		private String field;
		private boolean asc;
		private String ascv;

		private OrderField(String field, boolean asc) {
			this.field = field;
			this.asc = asc;
			this.ascv = asc ? "ASC" : "DESC";
		}

		public String getField() {
			return field;
		}

		public String getAsc() {
			return ascv;
		}

		public boolean desc() {
			return !asc;
		}
	}
}
