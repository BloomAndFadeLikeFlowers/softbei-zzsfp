package cn.softbei.po;

import java.io.Serializable;
import java.util.List;

public class NsrJXxNsridSimilarity implements Serializable {

	private String nsrid;
	private String nsridkpyf;
	private String jxnsridList;
	private String xxnsridList;

	public NsrJXxNsridSimilarity() {
		super();
		// TODO Auto-generated constructor stub
	}

	public NsrJXxNsridSimilarity(String nsridkpyf, String nsrid, String jxnsridList, String xxnsridList) {
		super();
		this.nsrid = nsrid;
		this.nsridkpyf = nsridkpyf;
		this.jxnsridList = jxnsridList;
		this.xxnsridList = xxnsridList;
	}

	@Override
	public String toString() {
		return nsridkpyf + "," + nsrid + "," + jxnsridList + "," + xxnsridList;
	}

	public String getNsridkpyf() {
		return nsridkpyf;
	}

	public void setNsridkpyf(String nsridkpyf) {
		this.nsridkpyf = nsridkpyf;
	}

	public String getNsrid() {
		return nsrid;
	}

	public void setNsrid(String nsrid) {
		this.nsrid = nsrid;
	}

	public String getJxnsridList() {
		return jxnsridList;
	}

	public void setJxnsridList(String jxnsridList) {
		this.jxnsridList = jxnsridList;
	}

	public String getXxnsridList() {
		return xxnsridList;
	}

	public void setXxnsridList(String xxnsridList) {
		this.xxnsridList = xxnsridList;
	}

}
