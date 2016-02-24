package net.butfly.albacore.dbo.criteria;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.support.Bean;
import net.butfly.albacore.utils.Objects;

public class Criteria extends Bean<Criteria> {
	private static final long serialVersionUID = 4775216639071589206L;

	private List<OrderField> orderFields;
	protected Map<String, Object> params;

	public Criteria() {
		this.params = new HashMap<String, Object>();
		this.orderFields = new ArrayList<OrderField>();
	};

	public Criteria setParameters(Map<String, ?> params) {
		this.params.putAll(params);
		return this;
	}

	public Criteria setEntity(AbstractEntity<?> entity) {
		this.params.putAll(Objects.toMap(entity));
		return this;
	}

	public Criteria set(String key, Object value) {
		this.params.put(key, value);
		return this;
	}

	public Map<String, Object> getParameters() {
		return this.params;
	}

	public OrderField[] getOrderFields() {
		return orderFields.toArray(new OrderField[this.orderFields.size()]);
	}

	public Criteria addOrder(String orderField, boolean asc) {
		this.orderFields.add(new OrderField(orderField, asc));
		return this;
	}

	public Criteria addOrder(String orderField) {
		this.orderFields.add(new OrderField(orderField));
		return this;
	}
}
