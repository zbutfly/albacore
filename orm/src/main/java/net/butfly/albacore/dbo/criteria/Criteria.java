package net.butfly.albacore.dbo.criteria;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.support.Bean;
import net.butfly.albacore.utils.ObjectUtils;

public class Criteria extends Bean<Criteria> {
	private static final long serialVersionUID = 4775216639071589206L;
	public static final String ORDER_FIELDS_PARAM_NAME = "orderFields";
	protected Map<String, Object> params;
	protected List<OrderField> orderFields;

	public List<OrderField> getOrderFields() {
		return orderFields;
	}

	public String getOrderBy() {
		if (orderFields.size() == 0) return null;
		StringBuilder sb = new StringBuilder(" ORDER BY");
		for (OrderField of : this.orderFields)
			sb.append(" ").append(of.field).append(" ").append(of.ascv);
		return sb.toString();
	}

	public Criteria() {
		this.params = new HashMap<String, Object>();
		this.orderFields = new ArrayList<Criteria.OrderField>();
	};

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

	public Criteria setEntity(AbstractEntity<?> entity) {
		this.params.putAll(ObjectUtils.toMap(entity));
		return this;
	}

	public Criteria set(String key, Object value) {
		this.params.put(key, value);
		return this;
	}

	public Map<String, Object> getParameters() {
		return this.params;
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
