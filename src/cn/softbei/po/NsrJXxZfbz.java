package cn.softbei.po;

import java.io.Serializable;

public class NsrJXxZfbz implements Serializable {

	private String nsrid;
	
	private String jxzfse;
	
	private String xxzfse;
	
	private String jxse;
	
	private String xxse;
	

	public NsrJXxZfbz(String nsrid, String jxzfse, String xxzfse, String jxse, String xxse) {
		super();
		this.nsrid = nsrid;
		this.jxzfse = jxzfse;
		this.xxzfse = xxzfse;
		this.jxse = jxse;
		this.xxse = xxse;

	}

	public NsrJXxZfbz() {
		super();
		// TODO Auto-generated constructor stub
	}

	
	@Override
	public String toString() {
		return nsrid + "," + jxzfse + "," + xxzfse + "," + jxse + "," + xxse;
	}

	public String getNsrid() {
		return nsrid;
	}

	public void setNsrid(String nsrid) {
		this.nsrid = nsrid;
	}

	public String getJxzfse() {
		return jxzfse;
	}

	public void setJxzfse(String jxzfse) {
		this.jxzfse = jxzfse;
	}

	public String getXxzfse() {
		return xxzfse;
	}

	public void setXxzfse(String xxzfse) {
		this.xxzfse = xxzfse;
	}

	public String getJxse() {
		return jxse;
	}

	public void setJxse(String jxse) {
		this.jxse = jxse;
	}

	public String getXxse() {
		return xxse;
	}

	public void setXxse(String xxse) {
		this.xxse = xxse;
	}
	
}
