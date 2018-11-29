package cn.softbei.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import cn.softbei.po.FeaturesLabel_final;
import cn.softbei.po.Hwmc;
import cn.softbei.po.NsrJXxSe;
import cn.softbei.po.Nsrxx;
import cn.softbei.po.Zzsfp;
import scala.Tuple2;

public class DataTransform_true implements Serializable {

	@Test
	public void left() throws AnalysisException {

		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("constructTemporaryFile1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<Nsrxx> nsrRow = sc.textFile("D://Data/softbei/Resource_test2/test_nsrxx")
				.map(new Function<String, Nsrxx>() {

					@Override
					public Nsrxx call(String arg0) throws Exception {
						String[] arr = arg0.split(",");
						return new Nsrxx(arr[0].substring(1), arr[1], arr[2], arr[3], arr[4],
								arr[5].substring(0, arr[5].length() - 1));
					}

				}).coalesce(1, true);

		JavaRDD<Row> nsrRow1 = sc.textFile("D://Data/softbei/Resource_test2/test_nsrxx_true")
				.map(new Function<String, Row>() {

					@Override
					public Row call(String arg0) throws Exception {
						// TODO Auto-generated method stub
						return RowFactory.create(arg0.split(",")[0]);
					}
				})

				.coalesce(1, true);
		List<StructField> structFields1 = new ArrayList<StructField>();
		// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
		structFields1.add(DataTypes.createStructField("nsrid_result", DataTypes.StringType, true));
		// 构建StructType,用于最后DataFrame元数据的描述
		StructType schema = DataTypes.createStructType(structFields1);
		sqlContext.createDataFrame(nsrRow1, schema).createTempView("nsrxx1");

		sqlContext.createDataFrame(nsrRow, Nsrxx.class).createTempView("nsrxx");

		JavaRDD<Row> nsr_jxx_yf_label_row = sqlContext.sql(
				"select hydm,nsrid,bzd,time1,time2,label from nsrxx1,nsrxx where nsrxx.nsrid = nsrxx1.nsrid_result")
				.javaRDD();

		// row对象文件
		nsr_jxx_yf_label_row.repartition(1).saveAsTextFile("D://Data/softbei/Resource_test2/nsrxx_new");
	}

	@Test
	public void tttttt() throws AnalysisException {

		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("constructTemporaryFile1");
		conf.set("spark.sql.crossJoin.enabled", "true");
		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<Nsrxx> nsrRow = sc.textFile("D://Data/softbei/Resource_test/test_nsrxx")
				.map(new Function<String, Nsrxx>() {

					@Override
					public Nsrxx call(String arg0) throws Exception {
						String[] arr = arg0.split(",");
						return new Nsrxx(arr[0].substring(1), arr[1], arr[2], arr[3], arr[4],
								arr[5].substring(0, arr[5].length() - 1));
					}

				}).coalesce(1, true);

		JavaRDD<Zzsfp> fpxx_row = sc.textFile("D://Data/softbei/Resource_test/test_fpxx")
				.map(new Function<String, Zzsfp>() {

					@Override
					public Zzsfp call(String arg0) throws Exception {
						// TODO Auto-generated method stub
						String[] arr = arg0.split(",");
						return new Zzsfp(arr[0].substring(1), arr[1], arr[2], arr[3], arr[4], arr[5], arr[6], arr[7],
								arr[8].substring(0, arr[8].length() - 1));
					}
				})

				.coalesce(1, true);

		sqlContext.createDataFrame(fpxx_row, Zzsfp.class).createOrReplaceTempView("fpxx");
		sqlContext.createDataFrame(nsrRow, Nsrxx.class).createOrReplaceTempView("nsrxx");

		JavaRDD<Row> nsr_jxx_yf_label_row = sqlContext.sql(
				"select fpid,gfid,xfid,je,se,jshj,kpyf,kpyf1,zfbz from fpxx,nsrxx where fpxx.xfid = nsrxx.nsrid")
				.javaRDD().coalesce(10);

		// row对象文件
		nsr_jxx_yf_label_row.map(new Function<Row, String>() {

			@Override
			public String call(Row arg0) throws Exception {
				// TODO Auto-generated method stub
				return "[" + new Zzsfp(arg0.getAs("fpid"), arg0.getAs("gfid"), arg0.getAs("xfid"), arg0.getAs("je"),
						arg0.getAs("se"), arg0.getAs("jshj"), arg0.getAs("kpyf"), arg0.getAs("kpyf1"),
						arg0.getAs("zfbz")).toString() + "]";
			}
		}).repartition(1).saveAsTextFile("D://Data/softbei/TTT/fpxx2");
	}
	
	
	@Test
	public void rrrrr() throws AnalysisException {

		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("constructTemporaryFile1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<Hwmc> hwmcRow = sc.textFile("D://Data/softbei/Resource_test/test_zzsfp_hwmx")
				.map(new Function<String, Hwmc>() {

					@Override
					public Hwmc call(String arg0) throws Exception {
						String[] arr = arg0.split(",");
						Hwmc h = null;
						try {
							h = new Hwmc(arr[0].substring(1), arr[1], arr[2], arr[3], arr[4],arr[5],arr[6],arr[7],arr[8],
									arr[9].substring(0, arr[9].length()-1));
						}
						catch (Exception e) {
							// TODO: handle exception
							System.out.println(arg0);
						}
						
						return h;
					}

				}).coalesce(1, true);

		JavaRDD<Row> fpxx = sc.textFile("D://Data/softbei/Resource_test/test_fpxx")
				.map(new Function<String, Row>() {

					@Override
					public Row call(String arg0) throws Exception {
						// TODO Auto-generated method stub
						return RowFactory.create(arg0.split(",")[0].substring(1));
					}
				})

				.coalesce(1, true);
		List<StructField> structFields1 = new ArrayList<StructField>();
		// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
		structFields1.add(DataTypes.createStructField("fpid", DataTypes.StringType, true));
		// 构建StructType,用于最后DataFrame元数据的描述
		StructType schema = DataTypes.createStructType(structFields1);
		sqlContext.createDataFrame(fpxx, schema).createTempView("fpxx");

		sqlContext.createDataFrame(hwmcRow, Hwmc.class).createTempView("hwmc");

		JavaRDD<Row> nsr_jxx_yf_label_row = sqlContext.sql(
				"select hwmc.fpid,t1,t2,t3,t4,t5,t6,t7,t8,t9 from fpxx,hwmc where fpxx.fpid = hwmc.fpid")
				.javaRDD();

		// row对象文件
		nsr_jxx_yf_label_row.repartition(1).saveAsTextFile("D://Data/softbei/TTT/hwmx");
	}
	
	@Test
	public void union() {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("constructTemporaryFile1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// zzsfp处理
		JavaRDD<String> zzsfp = sc.textFile("D://Data/softbei/Resource_test/test_fpxx")
				.union(sc.textFile("D://Data/softbei/Resource_test2/test_fpxx"));

		zzsfp.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, String>(arg0.split(",")[0],
						arg0.split(",")[1] + "," + arg0.split(",")[2] + "," + arg0.split(",")[3] + ","
								+ arg0.split(",")[4] + "," + arg0.split(",")[5] + "," + arg0.split(",")[6] + ","
								+ arg0.split(",")[7] + "," + arg0.split(",")[8]);
			}
		}).reduceByKey(new Function2<String, String, String>() {

			@Override
			public String call(String arg0, String arg1) throws Exception {

				return arg0;
			}
		}).repartition(1).saveAsTextFile("D://Data/softbei/Resource_test/test_fpxx_new");
		;
	}
}
