package cn.softbei.po;

import java.io.Serializable;
import java.util.List;

public class NsrJXxNsridSimilarity2 implements Serializable {

	private String nsrid;
	private String label;
	private List<String> jxnsridList;
	private List<String> xxnsridList;

	public NsrJXxNsridSimilarity2() {
		super();
		// TODO Auto-generated constructor stub
	}

	public NsrJXxNsridSimilarity2(String nsrid, String label, List<String> jxnsridList, List<String> xxnsridList) {
		super();
		this.nsrid = nsrid;
		this.label = label;
		this.jxnsridList = jxnsridList;
		this.xxnsridList = xxnsridList;
	}

	@Override
	public String toString() {
		return nsrid + "," + label + "," + jxnsridList + "," + xxnsridList;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public List<String> getJxnsridList() {
		return jxnsridList;
	}

	public void setJxnsridList(List<String> jxnsridList) {
		this.jxnsridList = jxnsridList;
	}

	public List<String> getXxnsridList() {
		return xxnsridList;
	}

	public void setXxnsridList(List<String> xxnsridList) {
		this.xxnsridList = xxnsridList;
	}

	public String getNsrid() {
		return nsrid;
	}

	public void setNsrid(String nsrid) {
		this.nsrid = nsrid;
	}

}
