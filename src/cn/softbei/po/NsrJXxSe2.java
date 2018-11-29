package cn.softbei.po;

import java.io.Serializable;

public class NsrJXxSe2 implements Serializable {

	private String nsrid_kpyf;
	private String nsrid;
	private String jxfpid;
	private String xxfpid;
	private String jxje;
	private String xxje;
	private String jxse;
	private String xxse;
	
	
	public NsrJXxSe2() {
		super();
		// TODO Auto-generated constructor stub
	}

	
	public NsrJXxSe2(String nsrid_kpyf, String nsrid, String jxfpid, String xxfpid, String jxje, String xxje,
			String jxse, String xxse) {
		super();
		this.nsrid = nsrid;
		this.nsrid_kpyf = nsrid_kpyf;
		this.jxfpid = jxfpid;
		this.xxfpid = xxfpid;
		this.jxje = jxje;
		this.xxje = xxje;
		this.jxse = jxse;
		this.xxse = xxse;
	}


	@Override
	public String toString() {
		return nsrid_kpyf + "," + nsrid + "," + jxfpid + "," + xxfpid + "," + jxje + "," + xxje + "," + jxse + ","
				+ xxse;
	}


	public String getNsrid() {
		return nsrid;
	}

	public void setNsrid(String nsrid) {
		this.nsrid = nsrid;
	}

	
	public String getNsrid_kpyf() {
		return nsrid_kpyf;
	}


	public void setNsrid_kpyf(String nsrid_kpyf) {
		this.nsrid_kpyf = nsrid_kpyf;
	}


	public String getJxfpid() {
		return jxfpid;
	}

	public void setJxfpid(String jxfpid) {
		this.jxfpid = jxfpid;
	}

	public String getXxfpid() {
		return xxfpid;
	}

	public void setXxfpid(String xxfpid) {
		this.xxfpid = xxfpid;
	}

	public String getJxje() {
		return jxje;
	}

	public void setJxje(String jxje) {
		this.jxje = jxje;
	}

	public String getXxje() {
		return xxje;
	}

	public void setXxje(String xxje) {
		this.xxje = xxje;
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
