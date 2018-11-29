package cn.softbei.po;

import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience.Private;

public class FeaturesLabel4 implements Serializable {

	private double nsrid;

	private double label;

	// 增值税专用发票用量变动异常 => （本期增值税额 - 上期增值税额）CV
	private double zzsfpqtyCV;

	// 税负变动率 => CV
	private double sfbdlCV;

	// 纳税人销售额变动率与应纳税额变动率弹性系数 => CV
	private double xsebdlzzsbdlCV;

	// （进项税额变动率-销项税额变动率）/销项税额变动率 => CV
	private double jxsexxseCV;
	
	// 运费发票抵扣进项占比 => CV
	private double jxyfsejxseCV;
}
