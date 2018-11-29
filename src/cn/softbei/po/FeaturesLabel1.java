package cn.softbei.po;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

public class FeaturesLabel1 implements Serializable {

	private double label;

	private String nsrid;
	
	private double xxchange;  //增值税专用发票用量变动异常
	
	private double jxchange;  //增值税专用发票用量变动异常
	
	private double zzschange;  //增值税专用发票用量变动异常

	private double sfchange;   //税负变动率异常
	
	private double jxseCV; // 进项税额波动情况

	private double xxseCV; // 销项税额波动情况

	private double zzsCV; // 增值税税额波动情况

	private double jxzfsezb; // 进项作废发票税额占进项总税额的比重

	private double xxzfsezb; // 销项作废发票税额占销项总税额的比重

	private double onlyOutputOrInput; // 有进无出或者有进无出
	
	private int numOfFp;
	
	private int numOfYf;

	public FeaturesLabel1() {
		super();
		// TODO Auto-generated constructor stub
	}


	public FeaturesLabel1(double label, String nsrid, double xxchange, double jxchange, double zzschange,
			double sfchange, double jxseCV, double xxseCV, double zzsCV, double jxzfsezb, double xxzfsezb,
			double onlyOutputOrInput, int numOfFp, int numOfYf) {
		super();
		this.label = label;
		this.nsrid = nsrid;
		this.xxchange = xxchange;
		this.jxchange = jxchange;
		this.zzschange = zzschange;
		this.sfchange = sfchange;
		this.jxseCV = jxseCV;
		this.xxseCV = xxseCV;
		this.zzsCV = zzsCV;
		this.jxzfsezb = jxzfsezb;
		this.xxzfsezb = xxzfsezb;
		this.onlyOutputOrInput = onlyOutputOrInput;
		this.numOfFp = numOfFp;
		this.numOfYf = numOfYf;
	}

	@Override
	public String toString() {
		return label + "," + nsrid + "," + xxchange + "," + jxchange + "," + zzschange + "," + sfchange + "," + jxseCV
				+ "," + xxseCV + "," + zzsCV + "," + jxzfsezb + "," + xxzfsezb + "," + onlyOutputOrInput + "," + numOfFp
				+ "," + numOfYf;
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

	public double getXxchange() {
		return xxchange;
	}



	public void setXxchange(double xxchange) {
		this.xxchange = xxchange;
	}



	public double getJxchange() {
		return jxchange;
	}


	public void setJxchange(double jxchange) {
		this.jxchange = jxchange;
	}


	public double getZzschange() {
		return zzschange;
	}


	public void setZzschange(double zzschange) {
		this.zzschange = zzschange;
	}


	public double getSfchange() {
		return sfchange;
	}

	public void setSfchange(double sfchange) {
		this.sfchange = sfchange;
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


	public int getNumOfFp() {
		return numOfFp;
	}


	public void setNumOfFp(int numOfFp) {
		this.numOfFp = numOfFp;
	}


	public int getNumOfYf() {
		return numOfYf;
	}


	public void setNumOfYf(int numOfYf) {
		this.numOfYf = numOfYf;
	}

}
