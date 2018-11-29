package cn.softbei.test;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import cn.softbei.po.FeaturesLabel;
import cn.softbei.po.FeaturesLabel0;
import cn.softbei.po.FeaturesLabel_old;
import cn.softbei.po.FeaturesLabel31;
import cn.softbei.po.FeaturesLabel_final;
import cn.softbei.po.NsrJXxSe2;
import cn.softbei.po.NsrJXxZfbz;
import scala.Tuple2;

public class FeatureTest1 implements Serializable {

	// NsrJXxSe ： nsrid_kpyf nsrid jxfpid xxfpid jxje xxje jxse xxse;
	@Test
	public void test1() throws AnalysisException {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("constructTemporaryFile1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// zzsfp处理
		JavaRDD<String> zzsfp = sc.textFile("D://Data/softbei/Resource2/zzsfp").cache();

		// 分离进项
		JavaPairRDD<String, NsrJXxZfbz> zzsfp_jx = zzsfp.mapToPair(new PairFunction<String, String, NsrJXxZfbz>() {

			@Override
			public Tuple2<String, NsrJXxZfbz> call(String arg0) throws Exception {
				// gfid
				String key = arg0.split(",")[2];
				String jxzfse;

				if (arg0.split(",")[8].equals("Y)"))
					jxzfse = arg0.split(",")[4];
				else
					jxzfse = "0";

				// nsrid jxzfse xxzfse jxse xxse;
				NsrJXxZfbz nsrJXxZfbz = new NsrJXxZfbz(arg0.split(",")[2], jxzfse, "0", arg0.split(",")[4], "0");
				return new Tuple2<String, NsrJXxZfbz>(key, nsrJXxZfbz);
			}
		}).coalesce(10);

		// 分离销项
		JavaPairRDD<String, NsrJXxZfbz> zzsfp_xx = zzsfp.mapToPair(new PairFunction<String, String, NsrJXxZfbz>() {

			@Override
			public Tuple2<String, NsrJXxZfbz> call(String arg0) throws Exception {
				// xfid
				String key = arg0.split(",")[1];
				String xxzfse;

				if (arg0.split(",")[8].equals("Y)"))
					xxzfse = arg0.split(",")[4];
				else
					xxzfse = "0";

				// nsrid jxzfse xxzfse jxse xxse;
				NsrJXxZfbz nsrJXxZfbz = new NsrJXxZfbz(arg0.split(",")[1], "0", xxzfse, "0", arg0.split(",")[4]);
				return new Tuple2<String, NsrJXxZfbz>(key, nsrJXxZfbz);
			}
		}).coalesce(10);

		// 进销项合并
		JavaRDD<NsrJXxZfbz> nsr_jxx_zfbz_row = zzsfp_jx.union(zzsfp_xx)
				.reduceByKey(new Function2<NsrJXxZfbz, NsrJXxZfbz, NsrJXxZfbz>() {

					@Override
					public NsrJXxZfbz call(NsrJXxZfbz arg0, NsrJXxZfbz arg1) throws Exception {
						NsrJXxZfbz nsrJXxZfbz = new NsrJXxZfbz();
						nsrJXxZfbz.setNsrid(arg0.getNsrid());

						// String string = arg0.getJxje(); //取整
						nsrJXxZfbz.setJxzfse(
								Double.parseDouble(arg0.getJxzfse()) + Double.parseDouble(arg1.getJxzfse()) + "");
						nsrJXxZfbz.setXxzfse(
								Double.parseDouble(arg0.getXxzfse()) + Double.parseDouble(arg1.getXxzfse()) + "");

						nsrJXxZfbz
								.setJxse(Double.parseDouble(arg0.getJxse()) + Double.parseDouble(arg1.getJxse()) + "");
						nsrJXxZfbz
								.setXxse(Double.parseDouble(arg0.getXxse()) + Double.parseDouble(arg1.getXxse()) + "");

						return nsrJXxZfbz;
					}
				}).map(new Function<Tuple2<String, NsrJXxZfbz>, NsrJXxZfbz>() {

					@Override
					public NsrJXxZfbz call(Tuple2<String, NsrJXxZfbz> arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0._2;
					}
				});

		// nsrxx规则化
		JavaRDD<Row> nsrRow = sc.textFile("D://Data/softbei/Resource2/nsrxx")
				.mapToPair(new PairFunction<String, String, String>() {

					@Override
					public Tuple2<String, String> call(String arg0) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, String>(arg0.split(",")[1], arg0.split(",")[5].charAt(0) + "");
					}
				}).reduceByKey(new Function2<String, String, String>() {

					@Override
					public String call(String arg0, String arg1) throws Exception {
						// TODO Auto-generated method stub
						return Integer.parseInt(arg0) + Integer.parseInt(arg1) + "";
					}
				}).map(new Function<Tuple2<String, String>, Row>() {

					@Override
					public Row call(Tuple2<String, String> arg0) throws Exception {
						String label = arg0._2.equals("0") ? "0" : "1";
						return RowFactory.create(arg0._1, label);
					}
				}).coalesce(10);

		List<StructField> structFields1 = new ArrayList<StructField>();
		// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
		structFields1.add(DataTypes.createStructField("nsrid", DataTypes.StringType, true));
		structFields1.add(DataTypes.createStructField("label", DataTypes.StringType, true));
		// 构建StructType,用于最后DataFrame元数据的描述
		StructType schema = DataTypes.createStructType(structFields1);
		sqlContext.createDataFrame(nsrRow, schema).createTempView("nsrxx");

		sqlContext.createDataFrame(nsr_jxx_zfbz_row, NsrJXxZfbz.class).createTempView("nsrjxxzfbz");

		JavaRDD<Row> nsr_jxx_zfbz_label_row = sqlContext
				.sql("select nsrjxxzfbz.*,label from nsrjxxzfbz,nsrxx where nsrxx.nsrid = nsrjxxzfbz.nsrid").javaRDD()
				.coalesce(1);

		JavaRDD<FeaturesLabel31> nsr_jxx_zfbz_label = nsr_jxx_zfbz_label_row.map(new Function<Row, FeaturesLabel31>() {

			@Override
			public FeaturesLabel31 call(Row arg0) throws Exception {

				double jxzfsezb = 1;
				double xxzfsezb = 1;

				if (Double.parseDouble(arg0.getAs("jxse").toString()) != 0)
					jxzfsezb = new BigDecimal(arg0.getAs("jxzfse").toString())
							.divide(new BigDecimal(arg0.getAs("jxse").toString()), 3, BigDecimal.ROUND_HALF_UP)
							.doubleValue();

				if (Double.parseDouble(arg0.getAs("xxse").toString()) != 0)
					xxzfsezb = new BigDecimal(arg0.getAs("xxzfse").toString())
							.divide(new BigDecimal(arg0.getAs("xxse").toString()), 3, BigDecimal.ROUND_HALF_UP)
							.doubleValue();
				// TODO Auto-generated method stub
				return new FeaturesLabel31(Double.parseDouble(arg0.getAs("nsrid").toString()),
						Double.parseDouble(arg0.getAs("label").toString()), jxzfsezb, xxzfsezb);
			}
		});

		sqlContext.createDataFrame(nsr_jxx_zfbz_label, FeaturesLabel31.class).createTempView("nsrjxxzfbzlabel3");

		// sqlContext.sql("select * from nsrjxxzfbzlabel").show();
		JavaRDD<FeaturesLabel_old> nsr_jxx_features_0_label = sc.textFile("D://Data/softbei/Test3/nsr_label_features")
				.map(new Function<String, FeaturesLabel_old>() {

					@Override
					public FeaturesLabel_old call(String arg0) throws Exception {

						return new FeaturesLabel_old(Double.parseDouble(arg0.split(",")[0]),
								Double.parseDouble(arg0.split(",")[1]), Double.parseDouble(arg0.split(",")[2]),
								Double.parseDouble(arg0.split(",")[3]), Double.parseDouble(arg0.split(",")[4]),
								Double.parseDouble(arg0.split(",")[5]), Double.parseDouble(arg0.split(",")[6]));
					}

				}).coalesce(1, true);

		sqlContext.createDataFrame(nsr_jxx_features_0_label, FeaturesLabel_old.class)
				.createTempView("nsrjxxzfbzlabel0");

		sqlContext
				.sql("select nsrjxxzfbzlabel0.*,jxzfsezb,xxzfsezb from nsrjxxzfbzlabel0 "
						+ "left join nsrjxxzfbzlabel3 on nsrjxxzfbzlabel0.nsrid = nsrjxxzfbzlabel3.nsrid")
				.toJavaRDD().map(new Function<Row, FeaturesLabel0>() {

					@Override
					public FeaturesLabel0 call(Row arg0) throws Exception {
						FeaturesLabel0 featuresLabel = new FeaturesLabel0();
						featuresLabel.setNsrid(arg0.getAs("nsrid"));
						featuresLabel.setLabel(arg0.getAs("label"));
						featuresLabel.setJxseCV(arg0.getAs("jxseCV"));
						featuresLabel.setXxseCV(arg0.getAs("xxseCV"));
						featuresLabel.setZzsCV(arg0.getAs("zzsCV"));
						if (arg0.getAs("jxzfsezb") != null)
							featuresLabel.setJxzfsezb(arg0.getAs("jxzfsezb"));
						else
							featuresLabel.setJxzfsezb(0);
						if (arg0.getAs("xxzfsezb") != null)
							featuresLabel.setXxzfsezb(arg0.getAs("xxzfsezb"));
						else
							featuresLabel.setXxzfsezb(0);
						featuresLabel.setJxxhwsimilarity(arg0.getAs("jxxhwsimilarity"));
						featuresLabel.setOnlyOutputOrInput(arg0.getAs("onlyOutputOrInput"));
						return featuresLabel;
					}
				}).repartition(1).saveAsTextFile("D://Data/softbei/Test4/nsr_label_features");
	}

	@Test
	public void saveCSVFile() throws IOException, AnalysisException {
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

//		 JavaRDD<FeaturesLabel> javaRDD =
//		 sc.textFile("D://Data/softbei/Test5/nsr_label_features")
		// .union(sc.textFile("D://Data/softbei/Test5/nsr_label_features_is1"))
		JavaRDD<FeaturesLabel> javaRDD = sc.textFile("D://Data/softbei/Test_res_1/nsr_label_features")
				// sc.textFile("D://Data/softbei/Test_res/nsr_label_features")
				.map(new Function<String, FeaturesLabel>() {

					@Override
					public FeaturesLabel call(String arg0) throws Exception {
						// TODO Auto-generated method stub
						String[] arr = arg0.split(",");
						return new FeaturesLabel(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]), arr[2],
								Double.parseDouble(arr[3]), Double.parseDouble(arr[4]), Double.parseDouble(arr[5]),
								Double.parseDouble(arr[6]), Double.parseDouble(arr[7]), Double.parseDouble(arr[8]),
								Double.parseDouble(arr[9]), Double.parseDouble(arr[10]), Double.parseDouble(arr[11]),
								Integer.parseInt(arr[12]), Integer.parseInt(arr[13]), Double.parseDouble(arr[14]),
								Double.parseDouble(arr[15]), Double.parseDouble(arr[16]), Double.parseDouble(arr[17]));
					}
				});
		Dataset<Row> nsr_label_features_train_df = sqlContext.createDataFrame(javaRDD, FeaturesLabel.class);

		nsr_label_features_train_df.repartition(1).write().format("com.databricks.spark.csv").option("header", "true")// 在csv第一行有属性"true"，没有就是"false"
				.option("delimiter", ",")// 默认以","分割
//				 .save("D://Data/softbei/Test5/CSV/nsr_label_features");
				.save("D://Data/softbei/Test_res_1/CSV/nsr_label_features");

	}

	// 特征文件分组，调整政府样本比例
	@Test
	public void tt() throws IOException, AnalysisException {
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<String> lines = sc.textFile("D://Data/softbei/Test4/nsr_label_features");

		lines.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Double.parseDouble(arg0.split(",")[0]) == 1;
			}
		}).repartition(1).saveAsTextFile("D://Data/softbei/Test4/nsr_label_features_is1");

		lines.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Double.parseDouble(arg0.split(",")[0]) == 0;
			}
		}).repartition(150).saveAsTextFile("D://Data/softbei/Test4/nsr_label_features_is0_150");

	}

}
