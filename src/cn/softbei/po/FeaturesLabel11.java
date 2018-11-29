package cn.softbei.po;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

public class FeaturesLabel11 implements Serializable {

	private double label;

	private double nsrid;  

	private double jxseCV; //进项税额波动情况

	private double xxseCV; //销项税额波动情况

	private double zzsCV; //增值税税额波动情况

	private double onlyOutputOrInput; //有进无出或者有进无出

	public FeaturesLabel11() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FeaturesLabel11(double label,double nsrid,  double jxseCV, double xxseCV, double zzsCV,
			double onlyOutputOrInput) {
		super();
		this.nsrid = nsrid;
		this.label = label;
		this.jxseCV = jxseCV;
		this.xxseCV = xxseCV;
		this.zzsCV = zzsCV;
		this.onlyOutputOrInput = onlyOutputOrInput;
	}

	@Override
	public String toString() {
		return label + "," + nsrid + "," + jxseCV + "," + xxseCV + "," + zzsCV + "," + onlyOutputOrInput;
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
