package cn.softbei.po;

import java.io.Serializable;

public class TFNP implements Serializable {
	private String ipath; // 预测当前模型时所用的0标签所在的索引
	private String TP; // 真正例/真阳性(True Positive, TP)
	private String FN; // 假负例/假阴性(False Negative, FN)
	private String FP; // 假正例/假阳性(False Positive, FP)
	private String TN; // 真负例/真阴性(True Negative, TN)
	private String P_1;//1为正样本时的精确率
	private String R_1;//1为正样本时的召回率
	private String f1score_1;//1为正样本时的F1Score
	private String P_0;//0为正样本时的精确率
	private String R_0;//0为正样本时的召回率
	private String f1score_0;//0为正样本时的F1Score

	public TFNP() {
		super();
		// TODO Auto-generated constructor stub
	}

	public TFNP(String ipath, String tP, String fN, String fP, String tN, String p_1, String r_1, String f1score_1,
			String p_0, String r_0, String f1score_0) {
		super();
		this.ipath = ipath;
		TP = tP;
		FN = fN;
		FP = fP;
		TN = tN;
		P_1 = p_1;
		R_1 = r_1;
		this.f1score_1 = f1score_1;
		P_0 = p_0;
		R_0 = r_0;
		this.f1score_0 = f1score_0;
	}

	@Override
	public String toString() {
		return ipath + "," + TP + "," + FN + "," + FP + "," + TN + "," + P_1 + "," + R_1 + "," + f1score_1 + "," + P_0
				+ "," + R_0 + "," + f1score_0;
	}

	public String getIpath() {
		return ipath;
	}

	public void setIpath(String ipath) {
		this.ipath = ipath;
	}

	public String getTP() {
		return TP;
	}

	public void setTP(String tP) {
		TP = tP;
	}

	public String getFN() {
		return FN;
	}

	public void setFN(String fN) {
		FN = fN;
	}

	public String getFP() {
		return FP;
	}

	public void setFP(String fP) {
		FP = fP;
	}

	public String getTN() {
		return TN;
	}

	public void setTN(String tN) {
		TN = tN;
	}

	public String getP_1() {
		return P_1;
	}

	public void setP_1(String p_1) {
		P_1 = p_1;
	}

	public String getR_1() {
		return R_1;
	}

	public void setR_1(String r_1) {
		R_1 = r_1;
	}

	public String getF1score_1() {
		return f1score_1;
	}

	public void setF1score_1(String f1score_1) {
		this.f1score_1 = f1score_1;
	}

	public String getP_0() {
		return P_0;
	}

	public void setP_0(String p_0) {
		P_0 = p_0;
	}

	public String getR_0() {
		return R_0;
	}

	public void setR_0(String r_0) {
		R_0 = r_0;
	}

	public String getF1score_0() {
		return f1score_0;
	}

	public void setF1score_0(String f1score_0) {
		this.f1score_0 = f1score_0;
	}

}
