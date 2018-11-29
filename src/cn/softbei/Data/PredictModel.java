package cn.softbei.Data;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import com.sun.org.apache.xml.internal.security.Init;

import cn.softbei.MyUtil.MyUtils;
import cn.softbei.po.FeaturesLabel;
import cn.softbei.po.FeaturesLabel0;
import cn.softbei.po.FeaturesLabel_final;
import cn.softbei.po.TFNP;
import scala.Tuple2;

public class PredictModel implements Serializable {

	private GBTClassificationModel model; // 预测模型


	public PredictModel() throws IOException {
		super();
		init();
	}

	/**
	 * 初始化模型
	 * @throws IOException 
	 */
	private void init() throws IOException {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("GBTAllFeatures");

		JavaSparkContext sc = new JavaSparkContext(conf);

		String path = "D://Data/softbei/FeaturesAndModel/final_model";

		// 从文件中读取已经训练好的模型
		if (new File(path).exists()) {
			this.model = GBTClassificationModel.load("D://Data/softbei/FeaturesAndModel/final_model");
			sc.close();
		}
		// 如果模型不存在，则训练模型
		else {
			sc.close();
			this.model = this.reTrain();
		}

	}

	// 获取模型
	public GBTClassificationModel getModel() {
		return model;

	}

	/**
	 * 测试有标签的评测数据
	 * 
	 * @param index
	 * @return List<String> "nsrid", "prediction","label"
	 */
	public List<String> predictHasLabel() {

		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("predictHasLabel");

		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<String> data = sc.textFile("D://Data/softbei/FeaturesAndModel/nsr_label_features_resTrue/part-00001");
//				sc.textFile("D://Data/softbei/FeaturesAndModel/nsr_label_features_is0_2/part-00001")
//						.union(sc.textFile("D://Data/softbei/FeaturesAndModel/nsr_label_features_is1"));

		// 加载并处理测试数据
		JavaRDD<FeaturesLabel_final> nsr_label_features = data

				.map(new Function<String, FeaturesLabel_final>() {
					@Override
					public FeaturesLabel_final call(String arg0) throws Exception {
						String[] arr = arg0.split(",");
						if (arr[2].contains("nsr"))
							return new FeaturesLabel_final(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]),
									arr[2], Double.parseDouble(arr[2].substring(3)), Double.parseDouble(arr[3]),
									Double.parseDouble(arr[4]), Double.parseDouble(arr[5]), Double.parseDouble(arr[6]),
									Double.parseDouble(arr[7]), Double.parseDouble(arr[8]), Double.parseDouble(arr[9]),
									Double.parseDouble(arr[10]), Double.parseDouble(arr[11]), Integer.parseInt(arr[12]),
									Integer.parseInt(arr[13]), Double.parseDouble(arr[14]), Double.parseDouble(arr[15]),
									Double.parseDouble(arr[16]), Double.parseDouble(arr[17]));
						return new FeaturesLabel_final(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]), arr[2],
								Double.parseDouble(arr[2]), Double.parseDouble(arr[3]), Double.parseDouble(arr[4]),
								Double.parseDouble(arr[5]), Double.parseDouble(arr[6]), Double.parseDouble(arr[7]),
								Double.parseDouble(arr[8]), Double.parseDouble(arr[9]), Double.parseDouble(arr[10]),
								Double.parseDouble(arr[11]), Integer.parseInt(arr[12]), Integer.parseInt(arr[13]),
								Double.parseDouble(arr[14]), Double.parseDouble(arr[15]), Double.parseDouble(arr[16]),
								Double.parseDouble(arr[17]));
					}

				}).coalesce(1, true);

		Dataset<Row> nsr_label_features_row = sqlContext.createDataFrame(nsr_label_features, FeaturesLabel_final.class);
		String[] featureCols_train = new String[] { "hydm", "xxchange", "jxchange", "zzschange", "sfchange", "jxseCV",
				"xxseCV", "zzsCV", "jxzfsezb", "xxzfsezb", "numOfFp", "numOfYf", "jxnsrsimilarity", "xxnsrsimilarity",
				"onlyOutputOrInput", "jxxhwsimilarity"};

		Dataset<Row> Data = new VectorAssembler().setInputCols(featureCols_train).setOutputCol("features")
				.transform(nsr_label_features_row);

		// use the random forest classifier to train (fit) the model
		Dataset<Row>[] splits = Data.randomSplit(new double[] { 0.7, 0.3 });
		Dataset<Row> test = splits[1];

		// 使用当前模型进行测试
		Dataset<Row> predictions2 = this.model.transform(Data);

		// 二分类评测
		BinaryClassificationMetrics metrics2 = new BinaryClassificationMetrics(
				predictions2.select("prediction", "label"));

		// 打印评测指标
		System.out
				.println("*****GBTClassifier************************************************************************");
		System.out.println("**************************************F1:  "
				+ metrics2.fMeasureByThreshold().toJavaRDD().collect().get(0)._2$mcD$sp() + "\n"
				+ "**************************************");
		
		List<String> res = predictions2.select("nsrid", "prediction","label").toJavaRDD().map(new Function<Row, String>() {

			@Override
			public String call(Row arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.getAs("nsrid").toString() + ","
				+ arg0.getAs("prediction").toString().split("\\.")[0] + ","
				+ arg0.getAs("label").toString().split("\\.")[0];
			}
		}).collect();

		sc.close();
		return res;
	}

	/**
	 * 测试没有标签的评测数据，
	 * 
	 * @return List<String> "nsrid", "prediction" 评测文件的预测结果，
	 *         默认保存在"D://Data/softbei/Test_res/nsr_label_features_predictions_NoLabel")
	 */
	 public List<String> predictNoLabel() {
		// 删除预测数据保存文件
		MyUtils.deleteDir(new File("D://Data/softbei/Test_res_final/nsr_label_features_predictions_NoLabel"));

		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("predictNoLabel");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

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

		// 处理测试数据
		Dataset<Row> nsr_label_features_test_df = sqlContext.createDataFrame(nsr_label_features_test_row,
				FeaturesLabel_final.class);
		String[] featureCols_test = new String[] { "hydm", "xxchange", "jxchange", "zzschange", "sfchange", "jxseCV",
				"xxseCV", "zzsCV", "jxzfsezb", "xxzfsezb", "numOfFp", "numOfYf", "jxnsrsimilarity", "xxnsrsimilarity",
				"onlyOutputOrInput", "jxxhwsimilarity" };
		Dataset<Row> testData = new VectorAssembler().setInputCols(featureCols_test).setOutputCol("features")
				.transform(nsr_label_features_test_df);

		// 使用当前模型进行测试

		Dataset<Row> predictions2 = this.model.transform(testData);

		// 保存预测文件
		JavaRDD<String> predictions_row2 = predictions2.select("nsrid", "prediction").toJavaRDD()
				.map(new Function<Row, String>() {

					@Override
					public String call(Row arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0.getAs("nsrid").toString() + "\t"
								+ arg0.getAs("prediction").toString().split("\\.")[0];
					}
				});
		predictions_row2.repartition(1)
				.saveAsTextFile("D://Data/softbei/Test_res_final/nsr_label_features_predictions_NoLabel");

		List<String> res = predictions2.select("nsrid", "prediction").toJavaRDD().map(new Function<Row, String>() {

			@Override
			public String call(Row arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.getAs("nsrid").toString() + "\t" + arg0.getAs("prediction").toString().split("\\.")[0];
			}
		}).collect();

		sc.close();
		return res;
	 }


	public GBTClassificationModel reTrain() throws IOException {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reTrain");

		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sc);
			
			JavaRDD<String> data = sc.textFile("D://Data/softbei/FeaturesAndModel/bestData/")
					.union(sc.textFile("D://Data/softbei/FeaturesAndModel/nsr_label_features_resTrue/part-00001"));
			
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

		Dataset<Row> nsr_label_features_train_df = sqlContext.createDataFrame(nsr_label_features_train_row,
				FeaturesLabel_final.class);
		// 创建特征向量
		String[] featureCols_train = new String[] { "hydm","xxchange", "jxchange", "zzschange", "sfchange",
				"jxseCV", "xxseCV", "zzsCV", "jxzfsezb","xxzfsezb","numOfFp", "numOfYf", "jxnsrsimilarity", "xxnsrsimilarity",
				"onlyOutputOrInput", "jxxhwsimilarity" };

		Dataset<Row> trainingData = new VectorAssembler().setInputCols(featureCols_train).setOutputCol("features")
				.transform(nsr_label_features_train_df);
	
		// create the classifier, set parameters for training
		GBTClassifier gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("features").setMaxIter(26)
				.setMaxDepth(15);

		// 梯度提升树
		GBTClassificationModel model2 = gbt.fit(trainingData);		

		 //保存模型

		 MyUtils.deleteDir(new File("D://Data/softbei/FeaturesAndModel/final_model"));
		 model2.save("D://Data/softbei/FeaturesAndModel/final_model");

		sc.close();
		this.model = model2;
		return model2;
	}
}
