package cn.softbei.po;

import java.io.Serializable;

public class Nsrxx implements Serializable{

	private String hydm;
	private String nsrid;
	private String bzd;
	private String time1;
	private String time2;
	private String label;
	
	public Nsrxx(String hydm, String nsrid, String bzd, String time1, String time2, String label) {
		super();
		this.hydm = hydm;
		this.nsrid = nsrid;
		this.bzd = bzd;
		this.time1 = time1;
		this.time2 = time2;
		this.label = label;
	}


	@Override
	public String toString() {
		return hydm + ","+ nsrid + ","+bzd + ","+time1 + ","+ time2+ ","+label;
	}


	public String getHydm() {
		return hydm;
	}
	public void setHydm(String hydm) {
		this.hydm = hydm;
	}
	public String getNsrid() {
		return nsrid;
	}
	public void setNsrid(String nsrid) {
		this.nsrid = nsrid;
	}
	public String getBzd() {
		return bzd;
	}
	public void setBzd(String bzd) {
		this.bzd = bzd;
	}
	public String getTime1() {
		return time1;
	}
	public void setTime1(String time1) {
		this.time1 = time1;
	}
	public String getTime2() {
		return time2;
	}
	public void setTime2(String time2) {
		this.time2 = time2;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	
}
