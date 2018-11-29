package cn.softbei.test;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.QuantileDiscretizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import cn.softbei.MyUtil.MyUtils;
import cn.softbei.po.FeaturesLabel;
import cn.softbei.po.FeaturesLabel_final;
import cn.softbei.po.TFNP;

public class Feature_transform implements Serializable {
	int i;

	@Test
	public void ttt() {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reTrain");

		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sc);

		MyUtils.deleteDir(new File("D://Data/softbei/Test6"));

		JavaRDD<String> javaRDD = sc.textFile("D://Data/softbei/Test5/nsr_label_features_is0_2/part-00000")
				.randomSplit(new double[] { 0.4, 0.6 })[0]
						.union(sc.textFile("D://Data/softbei/Test5/nsr_label_features_is1"))
						.union(sc.textFile("D://Data/softbei/Test5/nsr_label_features_is1"))
						.union(sc.textFile("D://Data/softbei/Test5/nsr_label_features_is1"));
		for (i = 1; i <= 10; i++) {
			javaRDD.map(new Function<String, FeaturesLabel>() {

				@Override
				public FeaturesLabel call(String arg0) throws Exception {
					String[] arr = arg0.split(",");
					return new FeaturesLabel(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]), arr[2],
							Double.parseDouble(arr[3]), Double.parseDouble(arr[4]), Double.parseDouble(arr[5]),
							Double.parseDouble(arr[6]), Double.parseDouble(arr[7]), Double.parseDouble(arr[8]),
							Double.parseDouble(arr[9]), Double.parseDouble(arr[10]), Double.parseDouble(arr[11]),
							Integer.parseInt(arr[12]), Integer.parseInt(arr[13]), Double.parseDouble(arr[14]),
							Double.parseDouble(arr[15]), Double.parseDouble(arr[16]), Double.parseDouble(arr[17]));
				}

			}).filter(new Function<FeaturesLabel, Boolean>() {

				@Override
				public Boolean call(FeaturesLabel arg0) throws Exception {
					return (arg0.getHydm() > ((i - 1) * 1000) && arg0.getHydm() <= ((i) * 1000));
				}
			}).coalesce(1, true).saveAsTextFile("D://Data/softbei/Test6/nsr_label_features_" + i + "000");
		}

	}

	@Test
	public void ttt1() {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reTrain");

		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sc);

		for (i = 1; i <= 10; i++) {
			sc.textFile("D://Data/softbei/Test_res_1/nsr_label_features").map(new Function<String, FeaturesLabel>() {

				@Override
				public FeaturesLabel call(String arg0) throws Exception {
					String[] arr = arg0.split(",");
					return new FeaturesLabel(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]), arr[2],
							Double.parseDouble(arr[3]), Double.parseDouble(arr[4]), Double.parseDouble(arr[5]),
							Double.parseDouble(arr[6]), Double.parseDouble(arr[7]), Double.parseDouble(arr[8]),
							Double.parseDouble(arr[9]), Double.parseDouble(arr[10]), Double.parseDouble(arr[11]),
							Integer.parseInt(arr[12]), Integer.parseInt(arr[13]), Double.parseDouble(arr[14]),
							Double.parseDouble(arr[15]), Double.parseDouble(arr[16]), Double.parseDouble(arr[17]));
				}

			}).filter(new Function<FeaturesLabel, Boolean>() {

				@Override
				public Boolean call(FeaturesLabel arg0) throws Exception {
					return (arg0.getHydm() > ((i - 1) * 1000) && arg0.getHydm() <= ((i) * 1000));
				}
			}).saveAsTextFile("D://Data/softbei/Test_res_2/nsr_label_features_" + i + "000");
		}

	}
}
