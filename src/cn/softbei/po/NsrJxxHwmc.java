package cn.softbei.po;

import java.io.Serializable;

public class NsrJxxHwmc implements Serializable {

	private String nsrid;
	private String jxhwmc;
	private String xxhwmc;

	public NsrJxxHwmc() {
		super();
		// TODO Auto-generated constructor stub
	}

	public NsrJxxHwmc(String nsrid, String jxhwmc, String xxhwmc) {
		super();
		this.nsrid = nsrid;
		this.jxhwmc = jxhwmc;
		this.xxhwmc = xxhwmc;
	}

	@Override
	public String toString() {
		return nsrid + "," + jxhwmc + "," + xxhwmc;
	}

	public String getNsrid() {
		return nsrid;
	}

	public void setNsrid(String nsrid) {
		this.nsrid = nsrid;
	}

	public String getJxhwmc() {
		return jxhwmc;
	}

	public void setJxhwmc(String jxhwmc) {
		this.jxhwmc = jxhwmc;
	}

	public String getXxhwmc() {
		return xxhwmc;
	}

	public void setXxhwmc(String xxhwmc) {
		this.xxhwmc = xxhwmc;
	}

}
