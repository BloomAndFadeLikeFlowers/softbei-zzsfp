package cn.softbei.po;

import java.io.Serializable;

public class FeaturesLabel_final implements Serializable {

	private double label;
	private String nsrid;
	private double nsrid_d;
	
	private double hydm;                 //行业代码
	private double xxchange;                // 增值税专用发票用量变动异常
	private double jxchange;               // 增值税专用发票用量变动异常
	private double zzschange;               // 增值税专用发票用量变动异常
	private double sfchange;                // 税负变动率异常
	private double jxseCV;                     //进项税额变异系数
	private double xxseCV;                  //销项税额变异系数
	private double zzsCV;             //增值税变异系数
	private double jxzfsezb;                // 进项作废发票税额占进项总税额的比重
	private double xxzfsezb;                    // 销项作废发票税额占销项总税额的比重
	private int    numOfFp;                      //发票总张数
	private int    numOfYf;                     //发票总月份数量
	private double jxnsrsimilarity;           // 进项nsr月平均相似度
	private double xxnsrsimilarity;         // 销项nsr月平均相似度
	private double onlyOutputOrInput;         //有进无出或有出无进
	private double jxxhwsimilarity;          //进销项货物相似度

	public FeaturesLabel_final() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FeaturesLabel_final(double label, double hydm, String nsrid, double nsrid_d, double xxchange,
			double jxchange, double zzschange, double sfchange, double jxseCV, double xxseCV, double zzsCV,
			double jxzfsezb, double xxzfsezb, int numOfFp, int numOfYf, double jxnsrsimilarity, double xxnsrsimilarity,
			double onlyOutputOrInput, double jxxhwsimilarity) {
		super();
		this.label = label;
		this.hydm = hydm;
		this.nsrid = nsrid;
		this.nsrid_d = nsrid_d;
		this.xxchange = xxchange;
		this.jxchange = jxchange;
		this.zzschange = zzschange;
		this.sfchange = sfchange;
		this.jxseCV = jxseCV;
		this.xxseCV = xxseCV;
		this.zzsCV = zzsCV;
		this.jxzfsezb = jxzfsezb;
		this.xxzfsezb = xxzfsezb;
		this.numOfFp = numOfFp;
		this.numOfYf = numOfYf;
		this.jxnsrsimilarity = jxnsrsimilarity;
		this.xxnsrsimilarity = xxnsrsimilarity;
		this.onlyOutputOrInput = onlyOutputOrInput;
		this.jxxhwsimilarity = jxxhwsimilarity;
	}

	@Override
	public String toString() {
		return label + "," + hydm + "," + nsrid + "," + nsrid_d + "," + xxchange + "," + jxchange + "," + zzschange
				+ "," + sfchange + "," + jxseCV + "," + xxseCV + "," + zzsCV + "," + jxzfsezb + "," + xxzfsezb + ","
				+ numOfFp + "," + numOfYf + "," + jxnsrsimilarity + "," + xxnsrsimilarity + "," + onlyOutputOrInput
				+ "," + jxxhwsimilarity;
	}

	public double getHydm() {
		return hydm;
	}

	public void setHydm(double hydm) {
		this.hydm = hydm;
	}

	public double getJxnsrsimilarity() {
		return jxnsrsimilarity;
	}

	public void setJxnsrsimilarity(double jxnsrsimilarity) {
		this.jxnsrsimilarity = jxnsrsimilarity;
	}

	public double getNsrid_d() {
		return nsrid_d;
	}

	public void setNsrid_d(double nsrid_d) {
		this.nsrid_d = nsrid_d;
	}

	public double getXxnsrsimilarity() {
		return xxnsrsimilarity;
	}

	public void setXxnsrsimilarity(double xxnsrsimilarity) {
		this.xxnsrsimilarity = xxnsrsimilarity;
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

	public void setXxzfsezb(double xxzfsezb) {
		this.xxzfsezb = xxzfsezb;
	}

	public double getJxxhwsimilarity() {
		return jxxhwsimilarity;
	}

	public void setJxxhwsimilarity(double jxxhwsimilarity) {
		this.jxxhwsimilarity = jxxhwsimilarity;
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

	public double getOnlyOutputOrInput() {
		return onlyOutputOrInput;
	}

	public void setOnlyOutputOrInput(double onlyOutputOrInput) {
		this.onlyOutputOrInput = onlyOutputOrInput;
	}

}
