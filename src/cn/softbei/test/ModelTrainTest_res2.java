package cn.softbei.test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.QuantileDiscretizer;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;
import cn.softbei.MyUtil.MyUtils;
import cn.softbei.po.FeaturesLabel;
import cn.softbei.po.FeaturesLabel0;
import cn.softbei.po.FeaturesLabel_final;
import cn.softbei.po.TFNP;

public class ModelTrainTest_res2 implements Serializable {

	@Test
	public void ttt() throws IOException {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reTrain");

		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sc);

		sc.setLogLevel("ERROR");

		double maxF1 = 0;
		for (int i = 50; i <= 70; i++) {

			JavaRDD<String> data = sc.textFile("D://Data/softbei/Test5/nsr_label_features_is0_2/")
			 .randomSplit(new double[] { 0.25, 0.75 })[0]
			 .union(sc.textFile("D://Data/softbei/Test5/nsr_label_features_is1"));

			// 加载并处理训练数据
			JavaRDD<FeaturesLabel_final> nsr_label_features_train_row = data

					.map(new Function<String, FeaturesLabel_final>() {
						@Override
						public FeaturesLabel_final call(String arg0) throws Exception {
							String[] arr = arg0.split(",");
							if (arr[2].contains("nsr"))
								return new FeaturesLabel_final(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]),
										arr[2], Double.parseDouble(arr[2].substring(3)), Double.parseDouble(arr[3]),
										Double.parseDouble(arr[4]), Double.parseDouble(arr[5]),
										Double.parseDouble(arr[6]), Double.parseDouble(arr[7]),
										Double.parseDouble(arr[8]), Double.parseDouble(arr[9]),
										Double.parseDouble(arr[10]), Double.parseDouble(arr[11]),
										Integer.parseInt(arr[12]), Integer.parseInt(arr[13]),
										Double.parseDouble(arr[14]), Double.parseDouble(arr[15]),
										Double.parseDouble(arr[16]), Double.parseDouble(arr[17]));
							return new FeaturesLabel_final(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]),
									arr[2], Double.parseDouble(arr[2]), Double.parseDouble(arr[3]),
									Double.parseDouble(arr[4]), Double.parseDouble(arr[5]), Double.parseDouble(arr[6]),
									Double.parseDouble(arr[7]), Double.parseDouble(arr[8]), Double.parseDouble(arr[9]),
									Double.parseDouble(arr[10]), Double.parseDouble(arr[11]), Integer.parseInt(arr[12]),
									Integer.parseInt(arr[13]), Double.parseDouble(arr[14]), Double.parseDouble(arr[15]),
									Double.parseDouble(arr[16]), Double.parseDouble(arr[17]));
						}

					}).coalesce(1, true);
			// 测试数据
			JavaRDD<FeaturesLabel_final> nsr_label_features_test_row = sc
					.textFile("D://Data/softbei/Test_res_1/nsr_label_features")
					.map(new Function<String, FeaturesLabel_final>() {

						@Override
						public FeaturesLabel_final call(String arg0) throws Exception {
							String[] arr = arg0.split(",");
							return new FeaturesLabel_final(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]),
									arr[2], Double.parseDouble(arr[2].substring(3)), Double.parseDouble(arr[3]),
									Double.parseDouble(arr[4]), Double.parseDouble(arr[5]), Double.parseDouble(arr[6]),
									Double.parseDouble(arr[7]), Double.parseDouble(arr[8]), Double.parseDouble(arr[9]),
									Double.parseDouble(arr[10]), Double.parseDouble(arr[11]), Integer.parseInt(arr[12]),
									Integer.parseInt(arr[13]), Double.parseDouble(arr[14]), Double.parseDouble(arr[15]),
									Double.parseDouble(arr[16]), Double.parseDouble(arr[17]));
						}

					}).coalesce(1, true);

			Dataset<Row> nsr_label_features_train_df = sqlContext.createDataFrame(nsr_label_features_train_row,
					FeaturesLabel_final.class);
			String[] featureCols_train = new String[] { "hydm", "xxchange", "jxchange", "zzschange", "sfchange",
					"jxseCV", "xxseCV", "zzsCV", "jxzfsezb", "xxzfsezb", "numOfFp", "numOfYf", "jxnsrsimilarity",
					"xxnsrsimilarity", "onlyOutputOrInput", "jxxhwsimilarity" };

			Dataset<Row> trainingData = new VectorAssembler().setInputCols(featureCols_train).setOutputCol("features")
					.transform(nsr_label_features_train_df);
			// 处理测试数据
			Dataset<Row> nsr_label_features_test_df = sqlContext.createDataFrame(nsr_label_features_test_row,
					FeaturesLabel_final.class);
			String[] featureCols_test = new String[] { "hydm", "xxchange", "jxchange", "zzschange", "sfchange",
					"jxseCV", "xxseCV", "zzsCV", "jxzfsezb", "xxzfsezb", "numOfFp", "numOfYf", "jxnsrsimilarity",
					"xxnsrsimilarity", "onlyOutputOrInput", "jxxhwsimilarity" };
			Dataset<Row> testData = new VectorAssembler().setInputCols(featureCols_test).setOutputCol("features")
					.transform(nsr_label_features_test_df);


			// create the classifier, set parameters for training
			GBTClassifier gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("features").setMaxIter(26)
					.setMaxDepth(15);

			// 交叉验证梯度提升树*************************************************************************
			Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { gbt });

			ParamMap[] paramGrid = new ParamGridBuilder()
//					.addGrid(gbt.maxIter(), new int[] {23,24,25,26,27,28})
//					.addGrid(gbt.maxDepth(), new int[] { 13,14,15,16,17,18 })
					.build();
			MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("label")
					.setPredictionCol("prediction")
					// "f1", "precision", "recall", "weightedPrecision", "weightedRecall"
					.setMetricName("f1");

			CrossValidator cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator)
					.setEstimatorParamMaps(paramGrid)
					.setNumFolds(3);
			CrossValidatorModel cvModel = cv.fit(trainingData);

			Dataset<Row> predictions3 = cvModel.transform(testData);



			// 二分类评测
			BinaryClassificationMetrics metrics3 = new BinaryClassificationMetrics(
					predictions3.select("prediction", "label"));

			// 打印评测指标
			System.out.println(
					i + "*****GBTClassifier************************************************************************");
			System.out.println("**************************************"
					+ metrics3.fMeasureByThreshold().toJavaRDD().collect().get(0)._2$mcD$sp() + "\n"
					+ "**************************************");
			if (maxF1 < (metrics3.fMeasureByThreshold().toJavaRDD().collect().get(0)._2$mcD$sp())) {
				maxF1 = metrics3.fMeasureByThreshold().toJavaRDD().collect().get(0)._2$mcD$sp();
				MyUtils.deleteDir(new File("D://Data/softbei/Test_res_3/bestData"));
				data.saveAsTextFile("D://Data/softbei/Test_res_3/bestData");
				// 保存模型
				MyUtils.deleteDir(new File("D://Data/softbei/Test_res_3/Model/cvModel"));
				cvModel.save("D://Data/softbei/Test_res_3/Model/cvModel");
			}

		}

		System.out.println(maxF1);
		sc.close();

	}
}
