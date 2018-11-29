package cn.softbei.po;

import java.io.Serializable;

public class FeaturesLabel2 implements Serializable{

	private String nsrid;
	
	private double label;
	
	private double jxxhwsimilarity; //进销项货物相似度

	public FeaturesLabel2() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FeaturesLabel2(String nsrid, double label, double jxxhwsimilarity) {
		super();
		this.nsrid = nsrid;
		this.label = label;
		this.jxxhwsimilarity = jxxhwsimilarity;
	}

	@Override
	public String toString() {
		return nsrid + "," + label + "," + jxxhwsimilarity;
	}

	public String getNsrid() {
		return nsrid;
	}

	public void setNsrid(String nsrid) {
		this.nsrid = nsrid;
	}

	public double getLabel() {
		return label;
	}

	public void setLabel(double label) {
		this.label = label;
	}

	public double getJxxhwsimilarity() {
		return jxxhwsimilarity;
	}

	public void setJxxhwsimilarity(double jxxhwsimilarity) {
		this.jxxhwsimilarity = jxxhwsimilarity;
	}

	

	
	
}
