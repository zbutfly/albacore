package net.butfly.demo.albacore.mapper;

import java.util.Date;

public class Permit extends PermitKey {
	private static final long serialVersionUID = 603334101957907689L;
	private Date permitDate;
	private Date expireDate;

	public Date getPermitDate() {
		return permitDate;
	}

	public void setPermitDate(Date permitDate) {
		this.permitDate = permitDate;
	}

	public Date getExpireDate() {
		return expireDate;
	}

	public void setExpireDate(Date expireDate) {
		this.expireDate = expireDate;
	}
}
