package cn.softbei.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.sun.org.apache.bcel.internal.generic.NEW;

import cn.softbei.MyUtil.MyUtils;
import cn.softbei.MyUtil.NsrCallable;
import cn.softbei.MyUtil.MyCallable;
import cn.softbei.po.FeaturesLabel;
import cn.softbei.po.FeaturesLabel0;
import cn.softbei.po.FeaturesLabel0;
import cn.softbei.po.FeaturesLabel1;
import cn.softbei.po.FeaturesLabel11;
import cn.softbei.po.FeaturesLabel2;
import cn.softbei.po.FeaturesLabel3;
import cn.softbei.po.NsrJXxNsridSimilarity;
import cn.softbei.po.NsrJXxNsridSimilarity2;
import cn.softbei.po.NsrJXxSe;
import cn.softbei.po.NsrJXxSe2;
import cn.softbei.po.NsrJxxHwmc;
import scala.Tuple2;

public class FeatureExtract implements Serializable {

	// 构造中间文件1，用于税额相关特征计算
	// NsrJXxSe ： nsrid_kpyf nsrid jxfpid xxfpid jxje xxje jxse xxse jxzfse xxzfse;
	@Test
	public void constructTemporaryFile1() throws AnalysisException {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("constructTemporaryFile1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// zzsfp处理
		JavaRDD<String> zzsfp = sc.textFile("D://Data/softbei/Resource2/zzsfp");

		// 分离进项
		JavaPairRDD<String, NsrJXxSe> zzsfp_jx = zzsfp.mapToPair(new PairFunction<String, String, NsrJXxSe>() {

			@Override
			public Tuple2<String, NsrJXxSe> call(String arg0) throws Exception {
				// gfid+kpyf
				String key = arg0.split(",")[2] + "_" + arg0.split(",")[6];
				String jxzfse = "0";
				if (arg0.split(",")[8].equals("Y)"))
					jxzfse = arg0.split(",")[4];

				// jxfpid xxfpid jxje xxje jxse xxse jxzfse xxzfse
				NsrJXxSe nsrJXxSe = new NsrJXxSe(arg0.split(",")[2] + "_" + arg0.split(",")[6], arg0.split(",")[2],
						arg0.split(",")[0].substring(6), "", arg0.split(",")[3], "0", arg0.split(",")[4], "0", jxzfse,
						"0");
				return new Tuple2<String, NsrJXxSe>(key, nsrJXxSe);
			}
		}).coalesce(10);

		// 分离销项
		JavaPairRDD<String, NsrJXxSe> zzsfp_xx = zzsfp.mapToPair(new PairFunction<String, String, NsrJXxSe>() {

			@Override
			public Tuple2<String, NsrJXxSe> call(String arg0) throws Exception {
				// xfid+kpyf
				String key = arg0.split(",")[1] + "_" + arg0.split(",")[6];
				String xxzfse = "0";
				if (arg0.split(",")[8].equals("Y)"))
					xxzfse = arg0.split(",")[4];

				// jxfpid xxfpid jxje xxje jxse xxse
				NsrJXxSe nsrJXxSe = new NsrJXxSe(arg0.split(",")[1] + "_" + arg0.split(",")[6], arg0.split(",")[1], "",
						arg0.split(",")[0].substring(6), "0", arg0.split(",")[3], "0", arg0.split(",")[4], "0", xxzfse);
				return new Tuple2<String, NsrJXxSe>(key, nsrJXxSe);
			}
		}).coalesce(10);

		// 进销项合并
		JavaRDD<NsrJXxSe> nsr_jxx_yf_row = zzsfp_jx.union(zzsfp_xx)
				.reduceByKey(new Function2<NsrJXxSe, NsrJXxSe, NsrJXxSe>() {

					@Override
					public NsrJXxSe call(NsrJXxSe arg0, NsrJXxSe arg1) throws Exception {
						NsrJXxSe nsrJXxSe = new NsrJXxSe();
						nsrJXxSe.setNsrid(arg0.getNsrid());
						nsrJXxSe.setNsrid_kpyf(arg0.getNsrid_kpyf());

						if (arg0.getJxfpid().length() > 0 && arg1.getJxfpid().length() > 0)
							nsrJXxSe.setJxfpid(arg0.getJxfpid() + "_" + arg1.getJxfpid());
						else if (arg0.getJxfpid().length() == 0 && arg1.getJxfpid().length() == 0)
							nsrJXxSe.setJxfpid("");
						else
							nsrJXxSe.setJxfpid(arg0.getJxfpid().length() > 0 ? arg0.getJxfpid() : arg1.getJxfpid());

						if (arg0.getXxfpid().length() > 0 && arg1.getXxfpid().length() > 0)
							nsrJXxSe.setXxfpid(arg0.getXxfpid() + "_" + arg1.getXxfpid());
						else if (arg0.getXxfpid().length() == 0 && arg1.getXxfpid().length() == 0)
							nsrJXxSe.setXxfpid("");
						else
							nsrJXxSe.setXxfpid(arg0.getXxfpid().length() > 0 ? arg0.getXxfpid() : arg1.getXxfpid());

						// String string = arg0.getJxje(); //取整
						nsrJXxSe.setJxje(Double.parseDouble(arg0.getJxje()) + Double.parseDouble(arg1.getJxje()) + "");
						nsrJXxSe.setXxje(Double.parseDouble(arg0.getXxje()) + Double.parseDouble(arg1.getXxje()) + "");

						nsrJXxSe.setJxse(Double.parseDouble(arg0.getJxse()) + Double.parseDouble(arg1.getJxse()) + "");
						nsrJXxSe.setXxse(Double.parseDouble(arg0.getXxse()) + Double.parseDouble(arg1.getXxse()) + "");

						nsrJXxSe.setJxzfse(
								Double.parseDouble(arg0.getJxzfse()) + Double.parseDouble(arg1.getJxzfse()) + "");
						nsrJXxSe.setXxzfse(
								Double.parseDouble(arg0.getXxzfse()) + Double.parseDouble(arg1.getXxzfse()) + "");

						return nsrJXxSe;
					}
				}).map(new Function<Tuple2<String, NsrJXxSe>, NsrJXxSe>() {

					@Override
					public NsrJXxSe call(Tuple2<String, NsrJXxSe> arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0._2;
					}
				}).coalesce(10);

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

		sqlContext.createDataFrame(nsr_jxx_yf_row, NsrJXxSe.class).createTempView("nsrjxxyf");

		JavaRDD<Row> nsr_jxx_yf_label_row = sqlContext.sql(
				"select nsrjxxyf.*,label from nsrxx left join nsrjxxyf on nsrxx.nsrid = split(nsrjxxyf.nsrid_kpyf,'_')[0]")
				.javaRDD();

		// row对象文件
		nsr_jxx_yf_label_row.repartition(10).saveAsObjectFile("D://Data/softbei/Test5/nsr_jxx_se_yf_label_big_object");

		// 排序后的文本文件
		nsr_jxx_yf_label_row.mapToPair(new PairFunction<Row, String, String>() {

			@Override
			public Tuple2<String, String> call(Row arg0) throws Exception {
				NsrJXxSe nsrJXxSe = new NsrJXxSe();
				nsrJXxSe.setNsrid(arg0.getAs("nsrid"));
				nsrJXxSe.setNsrid_kpyf(arg0.getAs("nsrid_kpyf"));
				nsrJXxSe.setJxfpid(arg0.getAs("jxfpid"));
				nsrJXxSe.setXxfpid(arg0.getAs("xxfpid"));
				nsrJXxSe.setJxje(arg0.getAs("jxje"));
				nsrJXxSe.setXxje(arg0.getAs("xxje"));
				nsrJXxSe.setJxse(arg0.getAs("jxse"));
				nsrJXxSe.setXxse(arg0.getAs("xxse"));
				nsrJXxSe.setJxzfse(arg0.getAs("jxzfse"));
				nsrJXxSe.setXxzfse(arg0.getAs("xxzfse"));

				return new Tuple2<String, String>(arg0.getAs("nsrid_kpyf"),
						nsrJXxSe.toString() + "," + arg0.getAs("label"));
			}
		}).sortByKey().repartition(1).saveAsTextFile("D://Data/softbei/Test5/nsr_jxx_se_yf_label_big");

	}

	// 构造中间文件2,用于进销项货物相似度计算
	// nsrid jxhwmc xxhwmc
	@Test
	public void constructTemporaryFile2() throws AnalysisException {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();

		conf.setMaster("local");
		conf.setAppName("constructTemporaryFile2");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// zzsfp处理
		JavaRDD<Row> zzsfprow = sc.textFile("D://Data/softbei/Resource2/zzsfp").coalesce(10)
				.mapToPair(new PairFunction<String, String, String>() {

					@Override
					public Tuple2<String, String> call(String arg0) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, String>(arg0.split(",")[0].substring(6),
								arg0.split(",")[1] + "_" + arg0.split(",")[2]);
					}
				}).distinct().map(new Function<Tuple2<String, String>, Row>() {

					@Override
					public Row call(Tuple2<String, String> arg0) throws Exception {
						// TODO Auto-generated method stub
						return RowFactory.create(arg0._1, arg0._2);
					}
				});

		List<StructField> structFields_zzsfp = new ArrayList<StructField>();
		// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
		structFields_zzsfp.add(DataTypes.createStructField("fpid", DataTypes.StringType, true));
		structFields_zzsfp.add(DataTypes.createStructField("gf_xf_nsrid", DataTypes.StringType, true));
		// 构建StructType,用于最后DataFrame元数据的描述
		StructType schema_zzsfp = DataTypes.createStructType(structFields_zzsfp);
		sqlContext.createDataFrame(zzsfprow, schema_zzsfp).createTempView("zzsfp");

		// hwmc处理
		JavaRDD<Row> hwmxrow = sc.textFile("D://Data/softbei/Resource2/zzsfp_hwmx").coalesce(10)
				.mapToPair(new PairFunction<String, String, Set<String>>() {

					@Override
					public Tuple2<String, Set<String>> call(String arg0) throws Exception {
						// TODO Auto-generated method stub
						Set<String> set = new HashSet<>();
						set.add(arg0.split(",")[2].replaceAll(" ", ""));
						return new Tuple2<String, Set<String>>(arg0.split(",")[0].substring(6), set);
					}
				}).reduceByKey(new Function2<Set<String>, Set<String>, Set<String>>() {

					@Override
					public Set<String> call(Set<String> arg0, Set<String> arg1) throws Exception {
						// TODO Auto-generated method stub
						arg0.addAll(arg1);
						return arg0;
					}
				}).map(new Function<Tuple2<String, Set<String>>, Row>() {

					@Override
					public Row call(Tuple2<String, Set<String>> arg0) throws Exception {
						// TODO Auto-generated method stub
						return RowFactory.create(arg0._1, arg0._2.toString());
					}
				});

		List<StructField> structFields_hwmx = new ArrayList<StructField>();
		// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
		structFields_hwmx.add(DataTypes.createStructField("fpid", DataTypes.StringType, true));
		structFields_hwmx.add(DataTypes.createStructField("hwmc", DataTypes.StringType, true));
		// 构建StructType,用于最后DataFrame元数据的描述
		StructType schema_hwmx = DataTypes.createStructType(structFields_hwmx);
		sqlContext.createDataFrame(hwmxrow, schema_hwmx).createTempView("hwmx");

		JavaRDD<Row> zzsfp_hwmc = sqlContext
				.sql("select zzsfp.*,hwmc from zzsfp left join hwmx on zzsfp.fpid = hwmx.fpid").toJavaRDD().coalesce(10)
				.cache();

		// 分离进项
		JavaPairRDD<String, NsrJxxHwmc> zzsfp_jx_hwmc = zzsfp_hwmc
				.mapToPair(new PairFunction<Row, String, NsrJxxHwmc>() {

					@Override
					public Tuple2<String, NsrJxxHwmc> call(Row arg0) throws Exception {
						// gfid
						String key = arg0.getAs("gf_xf_nsrid").toString().split("_")[0];
						// jxhwmc xxhwmc
						String hwmc = "";
						if (arg0.getAs("hwmc") != null)
							hwmc = arg0.getAs("hwmc");

						NsrJxxHwmc nsrJxxHwmc = new NsrJxxHwmc(key, hwmc, "");
						return new Tuple2<String, NsrJxxHwmc>(key, nsrJxxHwmc);

					}
				}).coalesce(10);

		// 分离销项
		JavaPairRDD<String, NsrJxxHwmc> zzsfp_xx_hwmc = zzsfp_hwmc
				.mapToPair(new PairFunction<Row, String, NsrJxxHwmc>() {

					@Override
					public Tuple2<String, NsrJxxHwmc> call(Row arg0) throws Exception {
						// gfid
						String key = arg0.getAs("gf_xf_nsrid").toString().split("_")[1];
						// jxhwmc xxhwmc
						String hwmc = "";
						if (arg0.getAs("hwmc") != null)
							hwmc = arg0.getAs("hwmc");
						NsrJxxHwmc nsrJxxHwmc = new NsrJxxHwmc(key, "", hwmc);
						return new Tuple2<String, NsrJxxHwmc>(key, nsrJxxHwmc);

					}
				}).coalesce(10);

		// 进销项合并
		JavaRDD<NsrJxxHwmc> nsr_jxx_hwmc_row = zzsfp_xx_hwmc.union(zzsfp_jx_hwmc)
				.reduceByKey(new Function2<NsrJxxHwmc, NsrJxxHwmc, NsrJxxHwmc>() {

					@Override
					public NsrJxxHwmc call(NsrJxxHwmc arg0, NsrJxxHwmc arg1) throws Exception {

						String Jxhwmc0 = arg0.getJxhwmc();
						String Jxhwmc1 = arg1.getJxhwmc();
						String Xxhwmc0 = arg0.getXxhwmc();
						String Xxhwmc1 = arg1.getXxhwmc();
						Set<String> jxSet = new HashSet<>();
						if (Jxhwmc0.replaceAll(" ", "").length() > 2)
							jxSet.addAll(Arrays.asList(Jxhwmc0.substring(1, Jxhwmc0.length() - 1).split(",")));

						if (Jxhwmc1.replaceAll(" ", "").length() > 2)
							jxSet.addAll(Arrays.asList(Jxhwmc1.substring(1, Jxhwmc1.length() - 1).split(",")));

						Set<String> xxSet = new HashSet<>();
						if (Xxhwmc0.replaceAll(" ", "").length() > 2)
							xxSet.addAll(Arrays.asList(Xxhwmc0.substring(1, Xxhwmc0.length() - 1).split(",")));
						if (Xxhwmc1.replaceAll(" ", "").length() > 2)
							xxSet.addAll(Arrays.asList(Xxhwmc1.substring(1, Xxhwmc1.length() - 1).split(",")));

						arg0.setJxhwmc(jxSet.toString());
						arg0.setJxhwmc(xxSet.toString());
						return arg0;
					}
				}).map(new Function<Tuple2<String, NsrJxxHwmc>, NsrJxxHwmc>() {

					@Override
					public NsrJxxHwmc call(Tuple2<String, NsrJxxHwmc> arg0) throws Exception {
						// TODO Auto-generated method stub
						arg0._2.setJxhwmc(arg0._2.getJxhwmc().replaceAll("\\[", "").replaceAll("\\]", "")
								.replaceAll(",", "").replaceAll(" ", ""));
						arg0._2.setXxhwmc(arg0._2.getXxhwmc().replaceAll("\\[", "").replaceAll("\\]", "")
								.replaceAll(",", "").replaceAll(" ", ""));
						return arg0._2;
					}
				}).coalesce(10);

		sqlContext.createDataFrame(nsr_jxx_hwmc_row, NsrJxxHwmc.class).createTempView("nsrjxxhwmc");

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
				});

		List<StructField> structFields1 = new ArrayList<StructField>();
		// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
		structFields1.add(DataTypes.createStructField("nsrid", DataTypes.StringType, true));
		structFields1.add(DataTypes.createStructField("label", DataTypes.StringType, true));
		// 构建StructType,用于最后DataFrame元数据的描述
		StructType schema = DataTypes.createStructType(structFields1);
		sqlContext.createDataFrame(nsrRow, schema).createTempView("nsrxx");
		JavaRDD<Row> lable_nsr_jxx_hwmc_row = sqlContext.sql(
				"select label,nsrjxxhwmc.nsrid,jxhwmc,xxhwmc from nsrjxxhwmc,nsrxx where nsrjxxhwmc.nsrid = nsrxx.nsrid")
				.toJavaRDD();
		// lable nsrid jxhwmc xxhwmc
		// row对象文件
		lable_nsr_jxx_hwmc_row.repartition(30).saveAsObjectFile("D://Data/softbei/Test5/nsr_jxx_hwmc_label_big_object");

		// 排序后的文本文件
		// lable_nsr_jxx_hwmc_row.repartition(1).saveAsTextFile("D://Data/softbei/Test5/nsr_jxx_hwmc_label_big");

	}

	// 构造中间文件3，用于每个月上下游公司的相似度计算
	// nsrid nsridkpyf jxnsridList xxnsridList
	@Test
	public void constructTemporaryFile3() throws AnalysisException {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("constructTemporaryFile3");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// zzsfp处理
		JavaRDD<String> zzsfp = sc.textFile("D://Data/softbei/Resource2/zzsfp");

		// 分离进项
		JavaPairRDD<String, NsrJXxNsridSimilarity> zzsfp_jx = zzsfp
				.mapToPair(new PairFunction<String, String, NsrJXxNsridSimilarity>() {

					@Override
					public Tuple2<String, NsrJXxNsridSimilarity> call(String arg0) throws Exception {
						String key = arg0.split(",")[2] + "_" + arg0.split(",")[6];
						// nsrid yf jxnsridList xxnsridList
						return new Tuple2<String, NsrJXxNsridSimilarity>(key,
								new NsrJXxNsridSimilarity(key, arg0.split(",")[2], arg0.split(",")[1], ""));
					}
				}).coalesce(10);

		// 分离销项
		JavaPairRDD<String, NsrJXxNsridSimilarity> zzsfp_xx = zzsfp
				.mapToPair(new PairFunction<String, String, NsrJXxNsridSimilarity>() {

					@Override
					public Tuple2<String, NsrJXxNsridSimilarity> call(String arg0) throws Exception {
						// xfid+kpyf
						String key = arg0.split(",")[1] + "_" + arg0.split(",")[6];

						return new Tuple2<String, NsrJXxNsridSimilarity>(key,
								new NsrJXxNsridSimilarity(key, arg0.split(",")[1], "", arg0.split(",")[2]));
					}
				}).coalesce(10);

		// 进销项合并
		JavaRDD<NsrJXxNsridSimilarity> nsr_jxx_yf_row = zzsfp_jx.union(zzsfp_xx)
				.reduceByKey(new Function2<NsrJXxNsridSimilarity, NsrJXxNsridSimilarity, NsrJXxNsridSimilarity>() {

					@Override
					public NsrJXxNsridSimilarity call(NsrJXxNsridSimilarity arg0, NsrJXxNsridSimilarity arg1)
							throws Exception {
						NsrJXxNsridSimilarity nsrJXxNsridSimilarity = new NsrJXxNsridSimilarity();
						nsrJXxNsridSimilarity.setNsrid(arg0.getNsrid());
						nsrJXxNsridSimilarity.setNsridkpyf(arg0.getNsridkpyf());

						if (!arg0.getJxnsridList().equals("") && !arg1.getJxnsridList().equals(""))
							nsrJXxNsridSimilarity.setJxnsridList(arg0.getJxnsridList() + "_" + arg1.getJxnsridList());
						else if (arg0.getJxnsridList().equals("") && arg1.getJxnsridList().equals(""))
							nsrJXxNsridSimilarity.setJxnsridList("");
						else
							nsrJXxNsridSimilarity.setJxnsridList(
									arg0.getJxnsridList().equals("") ? arg1.getJxnsridList() : arg0.getJxnsridList());
						if (!arg0.getXxnsridList().equals("") && !arg1.getXxnsridList().equals(""))
							nsrJXxNsridSimilarity.setXxnsridList(arg0.getXxnsridList() + "_" + arg1.getXxnsridList());
						else if (arg0.getXxnsridList().equals("") && arg1.getXxnsridList().equals(""))
							nsrJXxNsridSimilarity.setXxnsridList("");
						else
							nsrJXxNsridSimilarity.setXxnsridList(
									arg0.getXxnsridList().equals("") ? arg1.getXxnsridList() : arg0.getXxnsridList());
						return nsrJXxNsridSimilarity;
					}
				}).map(new Function<Tuple2<String, NsrJXxNsridSimilarity>, NsrJXxNsridSimilarity>() {

					@Override
					public NsrJXxNsridSimilarity call(Tuple2<String, NsrJXxNsridSimilarity> arg0) throws Exception {
						// TODO Auto-generated method stub
						arg0._2.setJxnsridList(arg0._2.getJxnsridList().replaceAll(" ", ""));
						arg0._2.setXxnsridList(arg0._2.getXxnsridList().replaceAll(" ", ""));
						return arg0._2;
					}
				}).coalesce(10);

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

		sqlContext.createDataFrame(nsr_jxx_yf_row, NsrJXxNsridSimilarity.class).createTempView("nsrjxxyf");

		JavaRDD<Row> nsr_jxx_yf_label_row = sqlContext
				.sql("select nsrjxxyf.*,label from nsrjxxyf,nsrxx where nsrxx.nsrid = split(nsrjxxyf.nsridkpyf,'_')[0]")
				.javaRDD();

		// row对象文件
		JavaRDD<NsrJXxNsridSimilarity2> nsr_jxx_yf_label_rdd = nsr_jxx_yf_label_row
				.mapToPair(new PairFunction<Row, String, NsrJXxNsridSimilarity2>() {

					@Override
					public Tuple2<String, NsrJXxNsridSimilarity2> call(Row arg0) throws Exception {

						return new Tuple2<String, NsrJXxNsridSimilarity2>(arg0.getAs("nsrid"),
								new NsrJXxNsridSimilarity2(arg0.getAs("nsrid"), arg0.getAs("label"),
										Arrays.asList(arg0.getAs("jxnsridList").toString()),
										Arrays.asList(arg0.getAs("xxnsridList").toString())));
					}
				}).reduceByKey(new Function2<NsrJXxNsridSimilarity2, NsrJXxNsridSimilarity2, NsrJXxNsridSimilarity2>() {

					@Override
					public NsrJXxNsridSimilarity2 call(NsrJXxNsridSimilarity2 arg0, NsrJXxNsridSimilarity2 arg1)
							throws Exception {

						List<String> list1 = new ArrayList<>();
						list1.addAll(arg0.getJxnsridList());
						list1.addAll(arg1.getJxnsridList());
						arg0.setJxnsridList(list1);

						List<String> list2 = new ArrayList<>();
						list2.addAll(arg0.getXxnsridList());
						list2.addAll(arg1.getXxnsridList());
						arg0.setXxnsridList(list2);
						return arg0;
					}
				}).map(new Function<Tuple2<String, NsrJXxNsridSimilarity2>, NsrJXxNsridSimilarity2>() {

					@Override
					public NsrJXxNsridSimilarity2 call(Tuple2<String, NsrJXxNsridSimilarity2> arg0) throws Exception {
						final CopyOnWriteArrayList<String> jxList = new CopyOnWriteArrayList<String>(
								arg0._2.getJxnsridList());
						for (String item : jxList) {
							if (item.equals("")) {
								jxList.remove(item);
							}
						}
						arg0._2.setJxnsridList(jxList);

						final CopyOnWriteArrayList<String> xxList = new CopyOnWriteArrayList<String>(
								arg0._2.getXxnsridList());
						for (String item : xxList) {
							if (item.equals("")) {
								xxList.remove(item);
							}
						}
						arg0._2.setXxnsridList(xxList);
						return arg0._2;
					}
				});
		nsr_jxx_yf_label_rdd.repartition(15)
				.saveAsObjectFile("D://Data/softbei/Test5/nsr_jxx_nsridsimilarity_yf_label_big_object");

		// 排序后的文本文件
		nsr_jxx_yf_label_rdd.repartition(1)
				.saveAsTextFile("D://Data/softbei/Test5/nsr_jxx_nsridsimilarity_yf_label_big");

	}

	// 计算特征1
	// FeaturesLabel1 ： nsrid label jxseCV xxseCV zzsCV jxzfsezb xxzfsezb
	// onlyOutput;

	// 计算特征1
	// FeaturesLabel1 ： nsrid label jxseCV xxseCV zzsCV jxzfsezb xxzfsezb
	// onlyOutput;
	@Test
	public void calculateFeature1() throws AnalysisException {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("calculateFeature1");

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<Row> nsr_jxx_yf_label_row = sc.objectFile("D://Data/softbei/Test5/nsr_jxx_se_yf_label_big_object")
				.map(new Function<Object, Row>() {

					@Override
					public Row call(Object arg0) throws Exception {
						// TODO Auto-generated method stub
						return (Row) arg0;
					}
				});
		// 特征1 FeaturesLabel1 ： nsrid label jxseCV xxseCV zzsCV onlyOutput;
		JavaRDD<FeaturesLabel1> nsr_label_features = nsr_jxx_yf_label_row
				.mapToPair(new PairFunction<Row, String, List<NsrJXxSe>>() {

					@Override
					public Tuple2<String, List<NsrJXxSe>> call(Row arg0) throws Exception {
						NsrJXxSe nsrJXxSe = new NsrJXxSe();
						nsrJXxSe.setNsrid(arg0.getAs("nsrid"));
						nsrJXxSe.setNsrid_kpyf(arg0.getAs("nsrid_kpyf"));
						nsrJXxSe.setJxfpid(arg0.getAs("jxfpid"));
						nsrJXxSe.setXxfpid(arg0.getAs("xxfpid"));
						nsrJXxSe.setJxje(arg0.getAs("jxje"));
						nsrJXxSe.setXxje(arg0.getAs("xxje"));
						nsrJXxSe.setJxse(arg0.getAs("jxse"));
						nsrJXxSe.setXxse(arg0.getAs("xxse"));
						nsrJXxSe.setJxzfse(arg0.getAs("jxzfse"));
						nsrJXxSe.setXxzfse(arg0.getAs("xxzfse"));
						return new Tuple2<String, List<NsrJXxSe>>(arg0.getAs("nsrid") + "_" + arg0.getAs("label"),
								Arrays.asList(nsrJXxSe));
					}
				}).reduceByKey(new Function2<List<NsrJXxSe>, List<NsrJXxSe>, List<NsrJXxSe>>() {

					@Override
					public List<NsrJXxSe> call(List<NsrJXxSe> arg0, List<NsrJXxSe> arg1) throws Exception {
						List<NsrJXxSe> list = new ArrayList<NsrJXxSe>();
						list.addAll(arg0);
						list.addAll(arg1);
						return list;
					}
				}).map(new Function<Tuple2<String, List<NsrJXxSe>>, FeaturesLabel1>() {

					@Override
					public FeaturesLabel1 call(Tuple2<String, List<NsrJXxSe>> arg0) throws Exception {
						FeaturesLabel1 featuresLabel = new FeaturesLabel1();
						List<String> jxseList = new ArrayList<>();
						List<String> xxseList = new ArrayList<>();
						List<String> zzsList = new ArrayList<>();
						String jxids = "";
						String xxids = "";
						BigDecimal jxzfseSum = new BigDecimal(0.0);
						BigDecimal xxzfseSum = new BigDecimal(0.0);
						BigDecimal jxseSum = new BigDecimal(0.0);
						BigDecimal xxseSum = new BigDecimal(0.0);
						for (NsrJXxSe nsrJXxSe : arg0._2) {
							jxseList.add(nsrJXxSe.getJxse());
							xxseList.add(nsrJXxSe.getXxse());

							jxzfseSum = jxzfseSum.add(new BigDecimal(nsrJXxSe.getJxzfse()));
							xxzfseSum = xxzfseSum.add(new BigDecimal(nsrJXxSe.getXxzfse()));
							jxseSum = jxseSum.add(new BigDecimal(nsrJXxSe.getJxse()));
							xxseSum = xxseSum.add(new BigDecimal(nsrJXxSe.getXxse()));

							jxids += nsrJXxSe.getJxfpid();
							xxids += nsrJXxSe.getXxfpid();
							zzsList.add(Double.parseDouble(nsrJXxSe.getXxse()) - Double.parseDouble(nsrJXxSe.getJxse())
									+ "");
						}
						featuresLabel.setNsrid(arg0._1.split("_")[0]);
						featuresLabel.setLabel(Double.parseDouble(arg0._1.split("_")[1]));
						featuresLabel.setSfchange(MyUtils.getSfChange(arg0._2));
						featuresLabel.setJxchange(MyUtils.getChange(jxseList));
						featuresLabel.setXxchange(MyUtils.getChange(xxseList));
						featuresLabel.setZzschange(MyUtils.getChange(zzsList));
						featuresLabel.setJxseCV(MyUtils.getCV(jxseList));
						featuresLabel.setXxseCV(MyUtils.getCV(xxseList));
						featuresLabel.setZzsCV(MyUtils.getCV(zzsList));
						featuresLabel.setNumOfFp(MyUtils.getNumOfFp(arg0._2));
						featuresLabel.setNumOfYf(arg0._2.size());
						if (jxseSum.doubleValue() != 0)
							featuresLabel
									.setJxzfsezb(jxzfseSum.divide(jxseSum, 3, BigDecimal.ROUND_HALF_UP).doubleValue());
						else
							featuresLabel.setJxzfsezb(1);
						if (xxseSum.doubleValue() != 0)
							featuresLabel
									.setXxzfsezb(jxzfseSum.divide(xxseSum, 3, BigDecimal.ROUND_HALF_UP).doubleValue());
						else
							featuresLabel.setXxzfsezb(1);

						// 有进有出，可能正常
						if (jxids.length() > 0 && xxids.length() > 0)
							featuresLabel.setOnlyOutputOrInput(0);
						else if (jxids.length() > 0 && xxids.length() == 0) // 有进无出，可能异常
							featuresLabel.setOnlyOutputOrInput(1);
						else if (jxids.length() == 0 && xxids.length() > 0) // 又出无进
							featuresLabel.setOnlyOutputOrInput(2);

						return featuresLabel;
					}
				});

		nsr_label_features.repartition(1).saveAsObjectFile("D://Data/softbei/Test5/nsr_label_features1_object");
		nsr_label_features.mapToPair(new PairFunction<FeaturesLabel1, String, FeaturesLabel1>() {

			@Override
			public Tuple2<String, FeaturesLabel1> call(FeaturesLabel1 arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, FeaturesLabel1>(arg0.getNsrid() + "", arg0);
			}
		}).sortByKey().repartition(1).saveAsTextFile("D://Data/softbei/Test5/nsr_label_features1");
		;

		// // 过滤标签为1的数据
		// nsr_label_features.filter(new Function<FeaturesLabel1, Boolean>() {
		//
		// @Override
		// public Boolean call(FeaturesLabel1 arg0) throws Exception {
		// // TODO Auto-generated method stub
		// return arg0.getLabel() == 1;
		// }
		// }).repartition(1).saveAsObjectFile("D://Data/softbei/Test5/nsr_label_features1_is1_object");
		// // 过滤标签为0的数据
		// nsr_label_features.filter(new Function<FeaturesLabel1, Boolean>() {
		//
		// @Override
		// public Boolean call(FeaturesLabel1 arg0) throws Exception {
		// // TODO Auto-generated method stub
		// return arg0.getLabel() == 0;
		// }
		// }).repartition(20).saveAsObjectFile("D://Data/softbei/Test5/nsr_label_features1_is0_object");
		//
		// nsr_label_features.filter(new Function<FeaturesLabel1, Boolean>() {
		//
		// @Override
		// public Boolean call(FeaturesLabel1 arg0) throws Exception {
		// // TODO Auto-generated method stub
		// return arg0.getLabel() == 1;
		// }
		// }).repartition(1).saveAsTextFile("D://Data/softbei/Test5/nsr_label_features1_is1");
		// nsr_label_features.filter(new Function<FeaturesLabel1, Boolean>() {
		//
		// @Override
		// public Boolean call(FeaturesLabel1 arg0) throws Exception {
		// // TODO Auto-generated method stub
		// return arg0.getLabel() == 0;
		// }
		// }).repartition(20).saveAsTextFile("D://Data/softbei/Test5/nsr_label_features1_is0");

	}

	int i = 0;

	// 计算特征2
	// FeaturesLabel2 ： nsrid label jxxhwsimilarity;
	@Test
	public void calculateFeature2() throws Exception {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("calculateFeature2");
		// conf.set("spark.driver.maxResultSize", "12g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		String path = "D://Data/softbei/Test5/nsr_jxx_hwmc_label_big_object/part-000";
		List<Tuple2<String, String>> nsrJxxHwmcRow = new ArrayList<>();

		// 创建一个线程池
		ExecutorService pool = Executors.newFixedThreadPool(15);
		int index = 0;
		List<Future> list = new ArrayList<Future>();
		for (i = index; i < index + 15; i++) {

			List<Row> temp = sc.objectFile(path + (i >= 10 ? i : "0" + i)).map(new Function<Object, Row>() {

				@Override
				public Row call(Object arg0) throws Exception {
					// TODO Auto-generated method stub
					return (Row) arg0;
				}

			}).collect();

			int step = temp.size() / 15;
			int flag = 1;
			for (int k = 0; k < temp.size() && flag == 1;) {
				List<Row> temp1 = new ArrayList<>();
				if (k + step <= temp.size()) {
					for (int j = 0; j < step; j++) {
						temp1.add(temp.get(k + j));
					}

					k = k + step;
				} else {
					step = temp.size() % 15;
					for (int j = 0; j < step; j++) {
						temp1.add(temp.get(k + j));
					}
					flag = 0;
				}

				// 创建多个有返回值的任务
				Callable c = new MyCallable(temp1, sc);
				// 执行任务并获取Future对象
				Future f = pool.submit(c);
				list.add(f);
			}

		}
		// 关闭线程池
		pool.shutdown();

		// 获取所有并发任务的运行结果
		for (Future f : list) {
			List<Tuple2<String, String>> temp = (List<Tuple2<String, String>>) f.get();
			// 从Future对象上获取任务的返回值，
			nsrJxxHwmcRow.addAll(temp);
		}

		JavaRDD<FeaturesLabel2> nsr_label_features2 = sc.parallelize(nsrJxxHwmcRow)
				.map(new Function<Tuple2<String, String>, FeaturesLabel2>() {

					@Override
					public FeaturesLabel2 call(Tuple2<String, String> arg0) throws Exception {
						// TODO Auto-generated method stub
						return new FeaturesLabel2(arg0._1.split("_")[0], Double.parseDouble(arg0._1.split("_")[1]),
								Double.parseDouble(arg0._2));
					}

				});

		// 保存特征2
		// nsr_label_features2_row.repartition(1).saveAsObjectFile("D://Data/softbei/Test3/nsr_label_features2_object");
		nsr_label_features2.repartition(1).saveAsTextFile("D://Data/softbei/Test5/nsr_label_features2/" + index);

	}

	// 计算特征3
	// FeaturesLabel2 ： nsrid label jxxhwsimilarity;
	@Test
	public void calculateFeature3() throws Exception {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("calculateFeature3");
		// conf.set("spark.driver.maxResultSize", "12g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		String path = "D://Data/softbei/Test5/nsr_jxx_nsridsimilarity_yf_label_big_object/part-000";
		List<Tuple2<String, String>> nsrJxxnsridRow = new ArrayList<>();

		// 创建一个线程池
		ExecutorService pool = Executors.newFixedThreadPool(15);
		int index = 7;
		List<Future> list = new ArrayList<Future>();
		for (i = index; i < index + 1; i++) {

			List<NsrJXxNsridSimilarity2> temp = sc.objectFile(path + (i >= 10 ? i : "0" + i))
					.map(new Function<Object, NsrJXxNsridSimilarity2>() {

						@Override
						public NsrJXxNsridSimilarity2 call(Object arg0) throws Exception {
							// TODO Auto-generated method stub
							return (NsrJXxNsridSimilarity2) arg0;
						}

					}).collect();

			int size = temp.size();
			int step = size / 15;
			int flag = 1;
			for (int k = 0; k < size && flag == 1;) {
				List<NsrJXxNsridSimilarity2> temp1 = new ArrayList<>();
				if (k + step <= size) {
					for (int j = 0; j < step; j++) {
						temp1.add(temp.get(k + j));
					}

					k = k + step;
				} else {
					step = size % 15;
					for (int j = 0; j < step; j++) {
						temp1.add(temp.get(k + j));
					}
					flag = 0;
				}

				// 创建多个有返回值的任务
				Callable c = new NsrCallable(temp1, sc);
				// 执行任务并获取Future对象
				Future f = pool.submit(c);
				list.add(f);
			}

		}
		// 关闭线程池
		pool.shutdown();

		// 获取所有并发任务的运行结果
		for (Future f : list) {
			List<Tuple2<String, String>> temp = (List<Tuple2<String, String>>) f.get();
			// 从Future对象上获取任务的返回值，
			nsrJxxnsridRow.addAll(temp);
		}

		JavaRDD<FeaturesLabel3> nsr_label_features2 = sc.parallelize(nsrJxxnsridRow)
				.map(new Function<Tuple2<String, String>, FeaturesLabel3>() {

					@Override
					public FeaturesLabel3 call(Tuple2<String, String> arg0) throws Exception {
						// TODO Auto-generated method stub
						return new FeaturesLabel3(arg0._1.split("_")[0], Double.parseDouble(arg0._1.split("_")[1]),
								Double.parseDouble(arg0._2.split("_")[0]), Double.parseDouble(arg0._2.split("_")[1]));
					}

				});

		// 保存特征3
		// nsr_label_features2_row.repartition(1).saveAsObjectFile("D://Data/softbei/Test3/nsr_label_features2_object");
		nsr_label_features2.repartition(1).saveAsTextFile("D://Data/softbei/Test5/nsr_label_features3/" + index);
	}

	// 特征1 2 3整合
	// FeaturesLabe： nsrid label jxseCV xxseCV zzsCV onlyOutput jxxhwjxxhwsimilarity
	@Test
	public void calculateFeaturesCombine() throws AnalysisException {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("calculateFeaturesCombine");

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// 获取特征1 label nsrid jxseCV xxseCV zzsCV onlyOutputOrInput
		JavaRDD<FeaturesLabel1> nsr_label_features1_row = sc
				.objectFile("D://Data/softbei/Test5/nsr_label_features1_object")
				.map(new Function<Object, FeaturesLabel1>() {

					@Override
					public FeaturesLabel1 call(Object arg0) throws Exception {
						// TODO Auto-generated method stub
						return (FeaturesLabel1) arg0;
					}
				});
		sqlContext.createDataFrame(nsr_label_features1_row, FeaturesLabel1.class).createTempView("nsr_label_features1");

		// 获取特征2 //nsrid label jxxhwsimilarity
		JavaRDD<FeaturesLabel2> nsr_label_features2_row = sc.textFile("D://Data/softbei/Test5/nsr_label_features2/")
				.map(new Function<String, FeaturesLabel2>() {

					@Override
					public FeaturesLabel2 call(String arg0) throws Exception {

						return new FeaturesLabel2(arg0.split(",")[0],
								new BigDecimal(arg0.split(",")[1])
										.divide(new BigDecimal(1.0), 4, BigDecimal.ROUND_HALF_UP).doubleValue(),
								new BigDecimal(arg0.split(",")[2])
										.divide(new BigDecimal(1.0), 4, BigDecimal.ROUND_HALF_UP).doubleValue());
					}
				});
		sqlContext.createDataFrame(nsr_label_features2_row, FeaturesLabel2.class).createTempView("nsr_label_features2");

		// 获取特征3 //nsrid label jxxhwsimilarity
		JavaRDD<FeaturesLabel3> nsr_label_features3_row = sc.textFile("D://Data/softbei/Test5/nsr_label_features3/")
				.map(new Function<String, FeaturesLabel3>() {

					@Override
					public FeaturesLabel3 call(String arg0) throws Exception {

						return new FeaturesLabel3(arg0.split(",")[0],
								new BigDecimal(arg0.split(",")[1])
										.divide(new BigDecimal(1.0), 4, BigDecimal.ROUND_HALF_UP).doubleValue(),
								new BigDecimal(arg0.split(",")[2])
										.divide(new BigDecimal(1.0), 4, BigDecimal.ROUND_HALF_UP).doubleValue(),
								new BigDecimal(arg0.split(",")[3])
										.divide(new BigDecimal(1.0), 4, BigDecimal.ROUND_HALF_UP).doubleValue());
					}
				});
		sqlContext.createDataFrame(nsr_label_features3_row, FeaturesLabel3.class).createTempView("nsr_label_features3");

		JavaRDD<FeaturesLabel0> nsr_label_features_row = sqlContext
				.sql("select nsr_label_features1.*,jxxhwsimilarity,jxnsrsimilarity,xxnsrsimilarity "
						+ "from nsr_label_features1,nsr_label_features2,nsr_label_features3 "
						+ "where nsr_label_features2.nsrid = nsr_label_features1.nsrid "
						+ "and nsr_label_features2.nsrid = nsr_label_features3.nsrid")
				.toJavaRDD().map(new Function<Row, FeaturesLabel0>() {

					@Override
					public FeaturesLabel0 call(Row arg0) throws Exception {
						// ouble label, String nsrid, double xxchange, double jxchange, double
						// zzschange,
						// double sfchange, double jxseCV, double xxseCV, double zzsCV, double jxzfsezb,
						// double xxzfsezb,
						// double jxnsrsimilarity, double xxnsrsimilarity, double onlyOutputOrInput,
						// double jxxhwsimilarity
						return new FeaturesLabel0(arg0.getAs("label"), arg0.getAs("nsrid"), arg0.getAs("xxchange"),
								arg0.getAs("jxchange"), arg0.getAs("zzschange"), arg0.getAs("sfchange"),
								arg0.getAs("jxseCV"), arg0.getAs("xxseCV"), arg0.getAs("zzsCV"), arg0.getAs("jxzfsezb"),
								arg0.getAs("xxzfsezb"), arg0.getAs("numOfFp"), arg0.getAs("numOfYf"),
								arg0.getAs("jxnsrsimilarity"), arg0.getAs("xxnsrsimilarity"),
								arg0.getAs("onlyOutputOrInput"), arg0.getAs("jxxhwsimilarity"));
					}
				});

		// 保存特征文件 label nsrid jxseCV xxseCV zzsCV jxzfsezb xxzfsezb jxnsrsimilarity
		// xxnsrsimilarity onlyOutputOrInput jxxhwsimilarity
		nsr_label_features_row.repartition(1).saveAsTextFile("D://Data/softbei/Test5/nsr_label_features0");

	}

	// 整合行业代码
	@Test
	public void FeaturesHydmCombine() throws AnalysisException {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("FeaturesHydmCombine");

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// 获取特征0 label nsrid jxseCV xxseCV zzsCV jxzfsezb xxzfsezb jxnsrsimilarity
		// xxnsrsimilarity onlyOutputOrInput jxxhwsimilarity
		JavaRDD<FeaturesLabel0> nsr_label_features0_row = sc.textFile("D://Data/softbei/Test5/nsr_label_features0")
				.map(new Function<String, FeaturesLabel0>() {

					@Override
					public FeaturesLabel0 call(String arg0) throws Exception {
						String[] arr = arg0.split(",");
						// TODO Auto-generated method stub
						return new FeaturesLabel0(Double.parseDouble(arr[0]), arr[1], Double.parseDouble(arr[2]),
								Double.parseDouble(arr[3]), Double.parseDouble(arr[4]), Double.parseDouble(arr[5]),
								Double.parseDouble(arr[6]), Double.parseDouble(arr[7]), Double.parseDouble(arr[8]),
								Double.parseDouble(arr[9]), Double.parseDouble(arr[10]), Integer.parseInt(arr[11]),
								Integer.parseInt(arr[12]), Double.parseDouble(arr[13]), Double.parseDouble(arr[14]),
								Double.parseDouble(arr[15]), Double.parseDouble(arr[16]));
					}
				}).coalesce(10);
		sqlContext.createDataFrame(nsr_label_features0_row, FeaturesLabel0.class).createTempView("nsr_label_features0");

		// 加载 nsrxx
		JavaRDD<Row> nsrRow = sc.textFile("D://Data/softbei/Resource2/nsrxx")
				.mapToPair(new PairFunction<String, String, String>() {

					@Override
					public Tuple2<String, String> call(String arg0) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, String>(arg0.split(",")[1] + "_" + arg0.split(",")[0].substring(1),
								"");
					}
				}).reduceByKey(new Function2<String, String, String>() {

					@Override
					public String call(String arg0, String arg1) throws Exception {
						// TODO Auto-generated method stub
						return arg0 + arg1;
					}
				}).map(new Function<Tuple2<String, String>, Row>() {

					@Override
					public Row call(Tuple2<String, String> arg0) throws Exception {
						// System.out.println("********************************************");
						// System.out.println(arg0);
						// System.out.println("********************************************");
						return RowFactory.create(arg0._1.toString().split("_")[0],
								Double.parseDouble(arg0._1.toString().split("_")[1]));
					}
				}).coalesce(10);

		List<StructField> structFields1 = new ArrayList<StructField>();
		// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
		structFields1.add(DataTypes.createStructField("nsrid", DataTypes.StringType, true));
		structFields1.add(DataTypes.createStructField("hydm", DataTypes.DoubleType, true));
		// 构建StructType,用于最后DataFrame元数据的描述
		StructType schema = DataTypes.createStructType(structFields1);
		sqlContext.createDataFrame(nsrRow, schema).createTempView("nsrxx");

		JavaRDD<FeaturesLabel> nsr_label_features_row = sqlContext
				.sql("select nsr_label_features0.*,hydm from nsr_label_features0,nsrxx "
						+ "where nsr_label_features0.nsrid = nsrxx.nsrid")
				.toJavaRDD().map(new Function<Row, FeaturesLabel>() {

					@Override
					public FeaturesLabel call(Row arg0) throws Exception {
						// TODO Auto-generated method stub
						return new FeaturesLabel(arg0.getAs("label"), arg0.getAs("hydm"), arg0.getAs("nsrid"),
								arg0.getAs("xxchange"), arg0.getAs("jxchange"), arg0.getAs("zzschange"),
								arg0.getAs("sfchange"), arg0.getAs("jxseCV"), arg0.getAs("xxseCV"), arg0.getAs("zzsCV"),
								arg0.getAs("jxzfsezb"), arg0.getAs("xxzfsezb"), arg0.getAs("numOfFp"),
								arg0.getAs("numOfYf"), arg0.getAs("jxnsrsimilarity"), arg0.getAs("xxnsrsimilarity"),
								arg0.getAs("onlyOutputOrInput"), arg0.getAs("jxxhwsimilarity"));
					}
				});
		// 保存特征文件 label hydm nsrid jxseCV xxseCV zzsCV jxzfsezb xxzfsezb jxnsrsimilarity
		// xxnsrsimilarity onlyOutputOrInput jxxhwsimilarity
		nsr_label_features_row.repartition(1).saveAsTextFile("D://Data/softbei/Test5/nsr_label_features");
		nsr_label_features_row.filter(new Function<FeaturesLabel, Boolean>() {

			@Override
			public Boolean call(FeaturesLabel arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.getLabel() == 1;
			}
		}).repartition(1).saveAsTextFile("D://Data/softbei/Test5/nsr_label_features_is1");

		nsr_label_features_row.filter(new Function<FeaturesLabel, Boolean>() {

			@Override
			public Boolean call(FeaturesLabel arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.getLabel() == 0;
			}
		}).repartition(2).saveAsTextFile("D://Data/softbei/Test5/nsr_label_features_is0_2");

	}

}
