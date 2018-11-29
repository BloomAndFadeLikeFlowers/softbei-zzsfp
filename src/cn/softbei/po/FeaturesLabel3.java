package cn.softbei.po;

import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience.Private;

public class FeaturesLabel3 implements Serializable {

	private String nsrid;

	private double label;

	private double jxnsrsimilarity; // 进项nsr月平均相似度

	private double xxnsrsimilarity; // 销项nsr月平均相似度

	public FeaturesLabel3() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FeaturesLabel3(String nsrid, double label, double jxnsrsimilarity, double xxnsrsimilarity) {
		super();
		this.nsrid = nsrid;
		this.label = label;
		this.jxnsrsimilarity = jxnsrsimilarity;
		this.xxnsrsimilarity = xxnsrsimilarity;
	}

	@Override
	public String toString() {
		return nsrid + "," + label + "," + jxnsrsimilarity + "," + xxnsrsimilarity;
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

	public double getJxnsrsimilarity() {
		return jxnsrsimilarity;
	}

	public void setJxnsrsimilarity(double jxnsrsimilarity) {
		this.jxnsrsimilarity = jxnsrsimilarity;
	}

	public double getXxnsrsimilarity() {
		return xxnsrsimilarity;
	}

	public void setXxnsrsimilarity(double xxnsrsimilarity) {
		this.xxnsrsimilarity = xxnsrsimilarity;
	}

}
