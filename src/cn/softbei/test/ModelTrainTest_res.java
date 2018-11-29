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

public class ModelTrainTest_res implements Serializable {

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
		for(int i = 24;i<=24;i++) {
			
			JavaRDD<String> data = sc.textFile("D://Data/softbei/Test_res_2/bestData/")
					.union(sc.textFile("D://Data/softbei/Test_res_1/nsr_label_features"));
//					.randomSplit(new double[] { 0.4, 0.6 })[0]
//							.union(sc.textFile("D://Data/softbei/Test5/nsr_label_features_is1"));
			
		// 加载并处理训练数据
		JavaRDD<FeaturesLabel_final> nsr_label_features_train_row = data


						.map(new Function<String, FeaturesLabel_final>() {
							@Override
							public FeaturesLabel_final call(String arg0) throws Exception {
								String[] arr = arg0.split(",");
								if (arr[2].contains("nsr"))
									return new FeaturesLabel_final(Double.parseDouble(arr[0]),
											Double.parseDouble(arr[1]), arr[2], Double.parseDouble(arr[2].substring(3)),
											Double.parseDouble(arr[3]), Double.parseDouble(arr[4]),
											Double.parseDouble(arr[5]), Double.parseDouble(arr[6]),
											Double.parseDouble(arr[7]), Double.parseDouble(arr[8]),
											Double.parseDouble(arr[9]), Double.parseDouble(arr[10]),
											Double.parseDouble(arr[11]), Integer.parseInt(arr[12]),
											Integer.parseInt(arr[13]), Double.parseDouble(arr[14]),
											Double.parseDouble(arr[15]), Double.parseDouble(arr[16]),
											Double.parseDouble(arr[17]));
								return new FeaturesLabel_final(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]),
										arr[2], Double.parseDouble(arr[2]), Double.parseDouble(arr[3]),
										Double.parseDouble(arr[4]), Double.parseDouble(arr[5]),
										Double.parseDouble(arr[6]), Double.parseDouble(arr[7]),
										Double.parseDouble(arr[8]), Double.parseDouble(arr[9]),
										Double.parseDouble(arr[10]), Double.parseDouble(arr[11]),
										Integer.parseInt(arr[12]), Integer.parseInt(arr[13]),
										Double.parseDouble(arr[14]), Double.parseDouble(arr[15]),
										Double.parseDouble(arr[16]), Double.parseDouble(arr[17]));
							}

						}).coalesce(1, true);
		// 测试数据
		JavaRDD<FeaturesLabel_final> nsr_label_features_test_row = sc
				.textFile("D://Data/softbei/Test_res_final/nsr_label_features")
				.map(new Function<String, FeaturesLabel_final>() {

					@Override
					public FeaturesLabel_final call(String arg0) throws Exception {
						String[] arr = arg0.split(",");
						return new FeaturesLabel_final(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]), arr[2],
								Double.parseDouble(arr[2].substring(3)), Double.parseDouble(arr[3]),
								Double.parseDouble(arr[4]), Double.parseDouble(arr[5]), Double.parseDouble(arr[6]),
								Double.parseDouble(arr[7]), Double.parseDouble(arr[8]), Double.parseDouble(arr[9]),
								Double.parseDouble(arr[10]), Double.parseDouble(arr[11]), Integer.parseInt(arr[12]),
								Integer.parseInt(arr[13]), Double.parseDouble(arr[14]), Double.parseDouble(arr[15]),
								Double.parseDouble(arr[16]), Double.parseDouble(arr[17]));
					}

				}).coalesce(1, true);

		Dataset<Row> nsr_label_features_train_df = sqlContext.createDataFrame(nsr_label_features_train_row,
				FeaturesLabel_final.class);
		// 创建特征向量
//		String[] featureCols_train = new String[] { "hydm", "nsrid_d", "xxchange", "jxchange", "zzschange", "sfchange",
//				"jxseCV", "xxseCV", "zzsCV", "jxzfsezb", "xxzfsezb", "jxnsrsimilarity","numOfYf",
//				"xxnsrsimilarity", "onlyOutputOrInput", "jxxhwsimilarity" };
		
//		String[] featureCols_train = new String[] { "hydm", "nsrid_d", "xxchange", "jxchange", "zzschange", "sfchange",
//				"jxseCV", "xxseCV", "zzsCV", "xxzfsezb", "jxnsrsimilarity", "numOfYf", "xxnsrsimilarity",
//				"onlyOutputOrInput", "jxxhwsimilarity" };   0.12随机森林
//		
//		String[] featureCols_train = new String[] { "hydm", "nsrid_d", "xxchange", "jxchange", "zzschange", "sfchange",
//				"jxseCV", "xxseCV", "zzsCV", "jxzfsezb", "jxnsrsimilarity", "numOfYf", "xxnsrsimilarity",
//				"onlyOutputOrInput", "jxxhwsimilarity" };   0.114提升树
		String[] featureCols_train = new String[] { "hydm","xxchange", "jxchange", "zzschange", "sfchange",
				"jxseCV", "xxseCV", "zzsCV", "jxzfsezb","xxzfsezb","numOfFp", "numOfYf", "jxnsrsimilarity", "xxnsrsimilarity",
				"onlyOutputOrInput", "jxxhwsimilarity" };

		Dataset<Row> trainingData = new VectorAssembler().setInputCols(featureCols_train).setOutputCol("features")
				.transform(nsr_label_features_train_df);
		// 处理测试数据
		Dataset<Row> nsr_label_features_test_df = sqlContext.createDataFrame(nsr_label_features_test_row,
				FeaturesLabel_final.class);
		String[] featureCols_test = new String[] { "hydm","xxchange", "jxchange", "zzschange", "sfchange",
				"jxseCV", "xxseCV", "zzsCV", "jxzfsezb","xxzfsezb", "numOfFp", "numOfYf", "jxnsrsimilarity", "xxnsrsimilarity",
				"onlyOutputOrInput", "jxxhwsimilarity" };
		Dataset<Row> testData = new VectorAssembler().setInputCols(featureCols_test).setOutputCol("features")
				.transform(nsr_label_features_test_df);

		// RFormula formula = new RFormula().setFormula(
		// "label ~
		// hydm+jxseCV+xxseCV+zzsCV+jxzfsezb+xxzfsezb+jxnsrsimilarity+xxnsrsimilarity+onlyOutputOrInput+jxxhwsimilarity")
		// .setFeaturesCol("features_new").setLabelCol("label");
		// Dataset<Row> output = formula.fit(trainingData).transform(trainingData);
		// output.select("features_new", "label").show();
		//

		// 特征离散化
		// Dataset<Row> result = new
		// QuantileDiscretizer().setInputCol("xxchange").setOutputCol("xxchange_new").setNumBuckets(3)//
		// 设置分箱数
		// .setRelativeError(0)// 设置precision-控制相对误差
		// .fit(nsr_label_features_train_df).transform(nsr_label_features_train_df);
		// Dataset<Row> result1 = new
		// QuantileDiscretizer().setInputCol("jxchange").setOutputCol("jxchange_new").setNumBuckets(3)//
		// 设置分箱数
		// .setRelativeError(0)// 设置precision-控制相对误差
		// .fit(result).transform(result);
		// Dataset<Row> result2 = new
		// QuantileDiscretizer().setInputCol("jxchange").setOutputCol("jxchange_new").setNumBuckets(3)//
		// 设置分箱数
		// .setRelativeError(0)// 设置precision-控制相对误差
		// .fit(result1).transform(result1);
		// Dataset<Row> result3 = new
		// QuantileDiscretizer().setInputCol("jxchange").setOutputCol("jxchange_new").setNumBuckets(3)//
		// 设置分箱数
		// .setRelativeError(0)// 设置precision-控制相对误差
		// .fit(result2).transform(result2);

		// // 卡方验证特征提取
		// ChiSqSelector selector = new
		// ChiSqSelector().setNumTopFeatures(10).setFeaturesCol("features")
		// .setLabelCol("label").setOutputCol("selectedFeatures");
		//
		// Dataset<Row> result = selector.fit(trainingData1).transform(trainingData1);
		// result.show();

		
		 // use the random forest classifier to train (fit) the model
		Dataset<Row>[] splits = trainingData.randomSplit(new double[] { 0.7, 0.3 });
		Dataset<Row> training = splits[0];
		Dataset<Row> test = splits[1];
		// create the classifier, set parameters for training
		RandomForestClassifier classifier = new RandomForestClassifier().setFeaturesCol("features").setLabelCol("label")
				.setImpurity("gini").setMaxDepth(25).setNumTrees(22).setFeatureSubsetStrategy("auto").setSeed(5043);

		GBTClassifier gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("features").setMaxIter(26)
				.setMaxDepth(15);

		// 随机森林
		RandomForestClassificationModel model = classifier.fit(trainingData);
		// 梯度提升树
		GBTClassificationModel model2 = gbt.fit(trainingData);
		// 使用当前模型进行测试

		Dataset<Row> predictions1 = model.transform(testData);
		Dataset<Row> predictions2 = model2.transform(testData);
		
		
	
		// 交叉验证梯度提升树*************************************************************************
//				 Pipeline pipeline=new Pipeline().
//				 setStages(new PipelineStage[] {gbt});
//				 
//				 ParamMap[] paramGrid = new ParamGridBuilder()
//						  .addGrid(gbt.maxIter(), new int[] {15,20,25})
//						  .addGrid(gbt.maxDepth(),new int[] {15,20,25})
//						  .build();
//				 MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
//				 .setLabelCol("label")
//				 .setPredictionCol("prediction")
//				 // "f1", "precision", "recall", "weightedPrecision", "weightedRecall"
//				 .setMetricName("f1");
//				 
//				 CrossValidator cv=new CrossValidator()
//				 .setEstimator(pipeline)
//				 .setEvaluator(evaluator)
//				 .setEstimatorParamMaps(paramGrid);
//				 .setNumFolds(10);
//				 CrossValidatorModel cvModel = cv.fit(trainingData);
//				 
//				 Dataset<Row> predictions3 = cvModel.transform(testData);
		
		// 提取"prediction", "label"，计算 TP FN FP TN P1 R1 F1Score1 P0 R0 F1Score0
		//保存预测文件
		MyUtils.deleteDir(new File("D://Data/softbei/Test_res_1/prediction1"));
		MyUtils.deleteDir(new File("D://Data/softbei/Test_res_1/prediction2"));
		MyUtils.deleteDir(new File("D://Data/softbei/Test_res_1/prediction3"));
		JavaRDD<Row> predictions_row1 = predictions1.select("nsrid","prediction").toJavaRDD();
		predictions_row1.saveAsTextFile("D://Data/softbei/Test_res_1/prediction1");
			JavaRDD<String> predictions_row2 = predictions2.select("nsrid", "prediction").toJavaRDD()
					.map(new Function<Row, String>() {

						@Override
						public String call(Row arg0) throws Exception {
							// TODO Auto-generated method stub
							return arg0.getAs("nsrid").toString() + "\t"
									+ arg0.getAs("prediction").toString().split("\\.")[0];
						}
					});
		predictions_row2.repartition(1).saveAsTextFile("D://Data/softbei/Test_res_1/prediction2");
//		JavaRDD<Row> predictions_row3 = predictions3.select("prediction", "label").toJavaRDD();
//		predictions_row3.saveAsTextFile("D://Data/softbei/Test_res_1/prediction3");
		// 1 1

		 //保存模型
//		 MyUtils.deleteDir(new File("D://Data/softbei/Test_res_1/Model/model"));
//		 MyUtils.deleteDir(new File("D://Data/softbei/Test_res_1/Model/model2"));
//		 MyUtils.deleteDir(new File("D://Data/softbei/Test_res_1/Model/cvModel"));
//		 model.save("D://Data/softbei/Test_res_1/Model/model");
//		 model2.save("D://Data/softbei/Test_res_1/Model/model2");
//		 cvModel.save("D://Data/softbei/Test_res_1/Model/cvModel");
		
		 //二分类评测
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(
				predictions1.select("prediction", "label"));
		BinaryClassificationMetrics metrics2 = new BinaryClassificationMetrics(
				predictions2.select("prediction", "label"));
//		BinaryClassificationMetrics metrics3 = new BinaryClassificationMetrics(
//				predictions3.select("prediction", "label"));
		// 打印评测指标

		System.out.println("\nRandomForestClassifier***************************************************************");
		System.out.println("**************************************"
				+ metrics.fMeasureByThreshold().toJavaRDD().collect().get(0)._2$mcD$sp() + "\n"
				+ "**************************************");
		
		// 打印评测指标
		System.out.println(i+"*****GBTClassifier************************************************************************");
		System.out.println("**************************************"
				+ metrics2.fMeasureByThreshold().toJavaRDD().collect().get(0)._2$mcD$sp() + "\n"
				+ "**************************************");
//		if(maxF1<(metrics2.fMeasureByThreshold().toJavaRDD().collect().get(0)._2$mcD$sp())) {
//				maxF1 = metrics2.fMeasureByThreshold().toJavaRDD().collect().get(0)._2$mcD$sp();
//				MyUtils.deleteDir(new File("D://Data/softbei/Test_res_2/bestData"));
//				data.saveAsTextFile("D://Data/softbei/Test_res_2/bestData");
//		}
		
//		System.out.println("MultilayerPerceptronClassifier***********************************************************************");
//		System.out.println("**************************************"
//				+ metrics3.fMeasureByThreshold().toJavaRDD().collect().toString() + "\n"
//				+ "**************************************"
//				+ metrics3.fMeasureByThreshold(0).toJavaRDD().collect().toString());
		
//		MyUtils.deleteDir(new File("D://Data/softbei/Test5/CSV/nsr_label_features_test"));
//		predictions2
//		.select("label","prediction","nsrid", "hydm", "xxchange", "jxchange", "zzschange", "sfchange",
//				"jxseCV", "xxseCV", "zzsCV", "jxzfsezb","xxzfsezb","numOfFp", "numOfYf", "jxnsrsimilarity", "xxnsrsimilarity",
//				"onlyOutputOrInput", "jxxhwsimilarity")
//		.repartition(1).write().format("com.databricks.spark.csv").option("header", "true")// 在csv第一行有属性"true"，没有就是"false"
//		.option("delimiter", ",")// 默认以","分割
//		.save("D://Data/softbei/Test5/CSV/nsr_label_features_test");
		}
		// predictions
		// .select("label","prediction","nsrid","hydm", "jxseCV", "xxseCV", "zzsCV",
		// "jxzfsezb", "xxzfsezb", "jxnsrsimilarity",
		// "xxnsrsimilarity", "onlyOutputOrInput", "jxxhwsimilarity")
		//
		// .repartition(1).write().format("com.databricks.spark.csv").option("header",
		// "true")// 在csv第一行有属性"true"，没有就是"false"
		// .option("delimiter", ",")// 默认以","分割
		// .save("D://Data/softbei/Test5/CSV/nsr_label_features_test");
		// predictions_row.saveAsTextFile("D://Data/softbei/Test5/prediction");
		System.out.println(maxF1);
		sc.close();
		
	}
}
