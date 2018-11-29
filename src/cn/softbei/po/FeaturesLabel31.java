package cn.softbei.po;

import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience.Private;

public class FeaturesLabel31 implements Serializable {

	private double nsrid;

	private double label;

	private double jxzfsezb; //进项作废发票税额占进项总税额的比重
	
	private double xxzfsezb; //销项作废发票税额占销项总税额的比重
	
	
	public FeaturesLabel31() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FeaturesLabel31(double nsrid, double label, double jxzfsezb, double xxzfsezb) {
		super();
		this.nsrid = nsrid;
		this.label = label;
		this.jxzfsezb = jxzfsezb;
		this.xxzfsezb = xxzfsezb;
	}

	@Override
	public String toString() {
		return nsrid + "," + label + "," + jxzfsezb + "," + xxzfsezb;
	}

	public double getNsrid() {
		return nsrid;
	}

	public void setNsrid(double nsrid) {
		this.nsrid = nsrid;
	}

	public double getLabel() {
		return label;
	}

	public void setLabel(double label) {
		this.label = label;
	}

	public double getJxzfsezb() {
		return jxzfsezb;
	}

	public void setJxzfsezb(double jxzfsezb) {
		this.jxzfsezb = jxzfsezb;
	}

	public double getXxzfsezb() {
		return xxzfsezb;
	}

	public void setXxzfsezb(double xxzfsezb) {
		this.xxzfsezb = xxzfsezb;
	}
	
	
	
}
