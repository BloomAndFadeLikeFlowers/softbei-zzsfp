package cn.softbei.po;

import java.io.Serializable;

public class FeaturesLabel_old implements Serializable {

	private double label;
	private double nsrid;

	private double jxseCV;

	private double xxseCV;

	private double zzsCV;

	private double onlyOutputOrInput;

	private double jxxhwsimilarity;

	public FeaturesLabel_old() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FeaturesLabel_old(double label,double nsrid,  double jxseCV, double xxseCV, double zzsCV,
			double onlyOutputOrInput, double jxxhwsimilarity) {
		super();
		this.nsrid = nsrid;
		this.label = label;
		this.jxseCV = jxseCV;
		this.xxseCV = xxseCV;
		this.zzsCV = zzsCV;
		this.onlyOutputOrInput = onlyOutputOrInput;
		this.jxxhwsimilarity = jxxhwsimilarity;
	}

	@Override
	public String toString() {
		return label + "," + nsrid + "," + jxseCV + "," + xxseCV + "," + zzsCV + "," + onlyOutputOrInput + ","
				+ jxxhwsimilarity;
	}

	public double getJxxhwsimilarity() {
		return jxxhwsimilarity;
	}

	public void setJxxhwsimilarity(double jxxhwsimilarity) {
		this.jxxhwsimilarity = jxxhwsimilarity;
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

	public double getJxseCV() {
		return jxseCV;
	}

	public void setJxseCV(double jxseCV) {
		this.jxseCV = jxseCV;
	}

	public double getXxseCV() {
		return xxseCV;
	}

	public void setXxseCV(double xxseCV) {
		this.xxseCV = xxseCV;
	}

	public double getZzsCV() {
		return zzsCV;
	}

	public void setZzsCV(double zzsCV) {
		this.zzsCV = zzsCV;
	}

	public double getOnlyOutputOrInput() {
		return onlyOutputOrInput;
	}

	public void setOnlyOutputOrInput(double onlyOutputOrInput) {
		this.onlyOutputOrInput = onlyOutputOrInput;
	}

}
