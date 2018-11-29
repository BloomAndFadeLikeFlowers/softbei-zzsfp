package cn.softbei.po;

import java.io.Serializable;

public class Zzsfp implements Serializable{
	private String fpid;
	private String gfid;
	private String xfid;
	private String je;
	private String se;
	private String jshj;
	private String kpyf;
	private String kpyf1;
	private String zfbz;
	
	
	public Zzsfp() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Zzsfp(String fpid, String gfid, String xfid, String je, String se, String jshj, String kpyf, String kpyf1,
			String zfbz) {
		super();
		this.fpid = fpid;
		this.gfid = gfid;
		this.xfid = xfid;
		this.je = je;
		this.se = se;
		this.jshj = jshj;
		this.kpyf = kpyf;
		this.kpyf1 = kpyf1;
		this.zfbz = zfbz;
	}
	@Override
	public String toString() {
		return fpid + "," + gfid + "," + xfid + "," + je + "," + se + "," + jshj + "," + kpyf + "," + kpyf1 + ","
				+ zfbz;
	}
	public String getFpid() {
		return fpid;
	}
	public void setFpid(String fpid) {
		this.fpid = fpid;
	}
	public String getGfid() {
		return gfid;
	}
	public void setGfid(String gfid) {
		this.gfid = gfid;
	}
	public String getXfid() {
		return xfid;
	}
	public void setXfid(String xfid) {
		this.xfid = xfid;
	}
	public String getJe() {
		return je;
	}
	public void setJe(String je) {
		this.je = je;
	}
	public String getSe() {
		return se;
	}
	public void setSe(String se) {
		this.se = se;
	}
	public String getJshj() {
		return jshj;
	}
	public void setJshj(String jshj) {
		this.jshj = jshj;
	}
	public String getKpyf() {
		return kpyf;
	}
	public void setKpyf(String kpyf) {
		this.kpyf = kpyf;
	}
	public String getKpyf1() {
		return kpyf1;
	}
	public void setKpyf1(String kpyf1) {
		this.kpyf1 = kpyf1;
	}
	public String getZfbz() {
		return zfbz;
	}
	public void setZfbz(String zfbz) {
		this.zfbz = zfbz;
	}
	
}
