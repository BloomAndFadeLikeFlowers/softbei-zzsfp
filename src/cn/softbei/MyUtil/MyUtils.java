package cn.softbei.MyUtil;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.ql.parse.HiveParser.recordReader_return;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bouncycastle.asn1.cmp.Challenge;
import org.junit.Test;
import org.lionsoul.jcseg.tokenizer.core.ADictionary;
import org.lionsoul.jcseg.tokenizer.core.DictionaryFactory;
import org.lionsoul.jcseg.tokenizer.core.ISegment;
import org.lionsoul.jcseg.tokenizer.core.IWord;
import org.lionsoul.jcseg.tokenizer.core.JcsegTaskConfig;
import org.lionsoul.jcseg.tokenizer.core.SegmentFactory;

import com.sun.org.apache.xml.internal.resolver.helpers.PublicId;

import cn.softbei.po.FeaturesLabel0;
import cn.softbei.po.NsrJXxSe;
import cn.softbei.po.NsrJxxHwmc;
import javafx.collections.ListChangeListener.Change;
import scala.Tuple2;
import scala.collection.mutable.DoubleLinkedList;

public class MyUtils implements Serializable {

	@Test
	public void tttt() throws Exception {
		System.out.println(getRandNum(0, 2));
		// System.out.println(getVariance(Arrays.asList("2522.0", "14558.9")));
		List<Tuple2<String, String>> res = new ArrayList<>();

		System.out.println(getCV(Arrays.asList("1", "2", "3")));
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		// conf.setMaster("spark://softbei:7077");
		conf.setMaster("local");
		conf.setAppName("test2");
		// conf.set("num-executors", "1");
		// conf.set("executor-memory", "6g");
		// conf.set("executor-cores", "2");
		// conf.set("driver-memory", "12g");

		// JavaSparkContext sc = new JavaSparkContext(conf);
		// System.out.println(getSimilarity(Arrays.asList(new NsrJxxHwmc("111",
		// "[[[涤纶梭织染色绣花布, 遥控器], [[物流辅助服务, [蓝源环保热源, [[(详见销货清单), 机动车交通事故责任强制保险], 螺纹钢]]]",
		// "[[物流辅助服务, [蓝源环保热源, [[(详见销货清单), 机动车交通事故责任强制保险], 螺纹钢]]], [[[[[铝塑复合袋, 运费],
		// PE穿线管],"
		// + " 涂层测厚仪], [[[纸扪头, [[生物质燃料, 高强瓦楞纸], 螺纹钢]], 手动平板闸阀带导流孔], [[[[碳元20#, 鞋],
		// [0#柴油, 围巾加工]], [3月份污水处理费, [[[[[优碳圆钢, [[胶棉套, [记号笔, 砼]], 洗衣粉]], 车用汽油], 快递费],
		// 维修费], 电费]]], 工具组合]]], [机动车商业行业示范汽车保险条款, 电影票]]]], 人棉梭织印花布],[, [[], [[[], [[[,
		// [, [, []]]], [, [[[]], [, [, [, [, [, [, [, [, []]]]]]]]]]]], [, [, []]]]],
		// [, [, [, []]]]]]]")),
		// sc));
	}

	public static List<Tuple2<String, String>> getSimilarity(List<NsrJxxHwmc> nsrJxxHwmcList, JavaSparkContext sc)
			throws Exception {
		List<Tuple2<String, String>> res = new ArrayList<>();
		for (NsrJxxHwmc nsrJxxHwmc : nsrJxxHwmcList) {
			double temp = getSimilarity1(Arrays.asList(nsrJxxHwmc.getJxhwmc(), nsrJxxHwmc.getXxhwmc(), ""), sc);
			res.add(new Tuple2<String, String>(nsrJxxHwmc.getNsrid(), temp + ""));
		}
		return res;
	}

	// 每月的上游公司和下游公司的相似度
	public static Double NsrSimilarity(List<String> list, JavaSparkContext sc) {
		List<TfIdfData> nsrList = new ArrayList<TfIdfData>();
		int k = 1;
		for (String string : list) {
			string = string.replaceAll("_", " ");
			nsrList.add(new TfIdfData(k + "", k + "", string));
			k++;
		}

		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<TfIdfData> rdd1 = sc.parallelize(nsrList);

		Dataset<Row> dataSet = sqlContext.createDataFrame(rdd1, TfIdfData.class);

		Dataset<Row> tfidfDataSet = MyUtils.tfidf(dataSet);

		// 用来比对的文本标号
		String id = "1";
		Row firstRow = tfidfDataSet.select("id", "title", "features").where("id ='" + id + "'").first();
		Vector firstFeatures = firstRow.getAs("features");
		Dataset<SimilartyData> similarDataset = tfidfDataSet.select("id", "title", "features")
				.map(new MapFunction<Row, SimilartyData>() {
					public SimilartyData call(Row row) {
						String id = row.getAs("id");
						String title = row.getAs("title");
						Vector features = row.getAs("features");
						double dot = BLAS.dot(firstFeatures.toSparse(), features.toSparse());
						double v1 = Vectors.norm(firstFeatures.toSparse(), 2.0);
						double v2 = Vectors.norm(features.toSparse(), 2.0);
						double similarty = dot / (v1 * v2);
						SimilartyData similartyData = new SimilartyData();
						similartyData.setId(id);
						similartyData.setTitle(title);
						similartyData.setSimilarty(similarty);
						return similartyData;
					}
				}, Encoders.bean(SimilartyData.class));
		Dataset<Row> similarDataset2 = sqlContext.createDataFrame(similarDataset.toJavaRDD(), SimilartyData.class);

		double res = 0;
		List<Double> similarList = similarDataset2.toJavaRDD().map(new Function<Row, Double>() {

			@Override
			public Double call(Row arg0) throws Exception {
				// TODO Auto-generated method stub

				return arg0.getAs("similarty");
			}
		}).collect();
		for (int i = 1; i < similarList.size(); i++) {
			res = res + similarList.get(i);
		}
		return new BigDecimal(res / (similarList.size() - 1)).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();

	}

	// 货物销项货物名称和进项货物名称相似度
	public static double getSimilarity1(List<String> Goods, JavaSparkContext sc) throws Exception {
		List<TfIdfData> list = divide(Goods);

		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<TfIdfData> rdd1 = sc.parallelize(list);

		Dataset<Row> dataSet = sqlContext.createDataFrame(rdd1, TfIdfData.class);

		Dataset<Row> tfidfDataSet = tfidf(dataSet);

		// 用来比对的文本标号
		String id = "2";

		Row firstRow = tfidfDataSet.select("id", "title", "features").where("id ='" + id + "'").first();
		Vector firstFeatures = firstRow.getAs("features");

		Dataset<SimilartyData> similarDataset = tfidfDataSet.select("id", "title", "features")
				.map(new MapFunction<Row, SimilartyData>() {
					public SimilartyData call(Row row) {
						String id = row.getAs("id");
						String title = row.getAs("title");
						Vector features = row.getAs("features");
						double dot = BLAS.dot(firstFeatures.toSparse(), features.toSparse());
						double v1 = Vectors.norm(firstFeatures.toSparse(), 2.0);
						double v2 = Vectors.norm(features.toSparse(), 2.0);
						double similarty = new BigDecimal(dot / (v1 * v2)).setScale(3, BigDecimal.ROUND_HALF_UP)
								.doubleValue();
						SimilartyData similartyData = new SimilartyData();
						similartyData.setId(id);
						similartyData.setTitle(title);
						similartyData.setSimilarty(similarty);
						return similartyData;
					}
				}, Encoders.bean(SimilartyData.class));
		Dataset<Row> similarDataset2 = sqlContext.createDataFrame(similarDataset.toJavaRDD(), SimilartyData.class);

		// similarDataset2.show();
		double res = 0;
		res = similarDataset2.toJavaRDD().first().getAs("similarty");
		return res;

	}

	// 增值税专用发票用量变动异常
	public static double getChange(List<String> data) {

		List<Double> newData = new ArrayList<>();

		for (int i = 1; i < data.size(); i++) {
			if (Double.parseDouble(data.get(i)) != 0)
				newData.add(Double.parseDouble(data.get(i)));
		}
		if (newData.isEmpty() || newData.size() < 2)
			return 0;
		double[] change = new double[newData.size()];
		for (int i = 1; i < newData.size(); i++) {
			change[i] = new BigDecimal(newData.get(i) - newData.get(i - 1))
					.divide(new BigDecimal(newData.get(i - 1)), 3, BigDecimal.ROUND_HALF_UP).abs().doubleValue();
		}

		double sum = 0;
		for (int i = 1; i < change.length; i++) {
			sum += change[i];
		}

		return new BigDecimal(sum / (newData.size() - 1)).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();

	}

	// 增值税一般纳税人税负变动异常
	public static double getSfChange(List<NsrJXxSe> data) {
		List<Double> sfList = new ArrayList<Double>();
		int i = 0;
		for (NsrJXxSe nsrJXxSe : data) {
			if (Double.parseDouble(nsrJXxSe.getJxse()) != 0.0 && Double.parseDouble(nsrJXxSe.getXxse()) != 0.0
					&& Double.parseDouble(nsrJXxSe.getXxje()) != 0) {
				double zzs = Double.parseDouble(nsrJXxSe.getXxse()) - Double.parseDouble(nsrJXxSe.getJxse());
				double jshj = Double.parseDouble(nsrJXxSe.getXxje()) + Double.parseDouble(nsrJXxSe.getXxse());
				if(jshj!=0) {
				double sf = new BigDecimal(zzs).divide(new BigDecimal(jshj), 3, BigDecimal.ROUND_HALF_UP).abs().doubleValue();
				if (sf != 0)
					sfList.add(sf);
				}
			}
		}
		if (sfList.size() < 2)
			return 0;
		// 求变动率数组
		double[] bdl = new double[sfList.size()];
		for (int j = 1; j < sfList.size(); j++) {
			bdl[j] = new BigDecimal((sfList.get(j) - sfList.get(j - 1))).abs()
					.divide(new BigDecimal(sfList.get(j - 1)), 3, BigDecimal.ROUND_HALF_UP).doubleValue();

		}
		double sum = 0;

		for (int j = 1; j < bdl.length; j++) {

			sum += bdl[j];
		}
		return new BigDecimal(sum / (bdl.length - 1)).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();

	}

	//获得总的发票张数
	public static int getNumOfFp(List<NsrJXxSe> data) {
		int numOfFp = 0;
		for (NsrJXxSe nsrJXxSe : data) {
			if(nsrJXxSe.getJxfpid()!="")
			numOfFp += nsrJXxSe.getJxfpid().split("_").length;
			if(nsrJXxSe.getXxfpid()!="")
			numOfFp += nsrJXxSe.getXxfpid().split("_").length;
		}
		return numOfFp;
		
	}
	
	
	// 变异系数=标准差/均值, 又称为相对标准差，符号为CV
	// 作用：反映单位均值上的离散程度，常用在两个总体均值不等的离散程度的比较上。
	// 若两个总体的均值相等，则比较标准差系数与比较标准差是等价的。有时变异系数表达为百分数的形式，即将CV值乘以100%
	// 获取变异系数
	public static double getCV(List<String> data) {

		BigDecimal avg = new BigDecimal(0.0);
		double n = data.size();
		BigDecimal sum = new BigDecimal(0.0);
		for (String string : data) {
			sum = sum.add(new BigDecimal(string));
		}
		avg = sum.divide(new BigDecimal(n), 3, BigDecimal.ROUND_HALF_UP);

		sum = new BigDecimal(0.0);
		for (String string : data) {
			sum = sum.add((new BigDecimal(string).subtract(avg)).pow(2));

		}
		BigDecimal variance = sum.divide(new BigDecimal(n), 3, BigDecimal.ROUND_HALF_UP);
		if (avg.doubleValue() != 0)
			return new BigDecimal(Math.sqrt(variance.doubleValue())).divide(avg, 3, BigDecimal.ROUND_HALF_UP)
					.doubleValue();
		else
			return 0.0;

	}

	// 获取方差
	public static double getVariance(List<String> data) {

		BigDecimal avg = new BigDecimal(0.0);
		double n = data.size();
		BigDecimal sum = new BigDecimal(0.0);
		for (String string : data) {
			sum = sum.add(new BigDecimal(string));
		}
		avg = sum.divide(new BigDecimal(n), 3, BigDecimal.ROUND_HALF_UP);
		sum = new BigDecimal(0.0);
		for (String string : data) {
			sum = sum.add((new BigDecimal(string).subtract(avg)).pow(2));

		}

		return sum.divide(new BigDecimal(n), 3, BigDecimal.ROUND_HALF_UP).doubleValue();

	}

	// 将词及词频转成dataframe
	public static Dataset<Row> tfidf(Dataset<Row> dataset) {
		Tokenizer tokenizer = new Tokenizer().setInputCol("segment").setOutputCol("words");
		Dataset<Row> wordsData = tokenizer.transform(dataset);
		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("features");
		Dataset<Row> featurizedData = hashingTF.transform(wordsData);
		// IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		// IDFModel idfModel = idf.fit(featurizedData);
		// Dataset<Row> rescaledData = idfModel.transform(featurizedData);
		return featurizedData;
	}

	// 分词操作
	public static List<TfIdfData> divide(List<String> Goods) throws Exception {
		ISegment seg = null;

		if (seg == null) {
			JcsegTaskConfig config = new JcsegTaskConfig();
			config.setLoadCJKPos(true);
			String path = "D:/Data/softbei/lexicon";
			// System.out.println(new File("").getAbsolutePath());
			ADictionary dic = DictionaryFactory.createDefaultDictionary(config);
			dic.loadDirectory(path);
			seg = SegmentFactory.createJcseg(JcsegTaskConfig.COMPLEX_MODE, config, dic);
		}

		// 保存文本的list
		List<TfIdfData> list = new ArrayList<>();
		int k = 1;
		for (String str : Goods) {
			seg.reset(new StringReader(str));
			StringBuffer sff = new StringBuffer();
			IWord word = seg.next();
			while (word != null) {

				sff.append(word.getValue()).append(" ");
				word = seg.next();
			}
			list.add(new TfIdfData(k + "", k + "", sff.toString()));
			k++;
		}
		return list;
	}

	// 获取指定长度和范围的随机数
	public static String getRandNum(int min, int max) {
		int result = min + (int) (Math.random() * ((max - 1 - min) + 1));
		String res = "";
		if (result >= 100)
			res = result + "";
		else if (result >= 10)
			res = "0" + result;
		else
			res = "00" + result;
		return res;

	}

	// 删除指定文件或指定文件夹及其下面所有文件
	public static void deleteDir(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles())
				deleteDir(f);
		}
		file.delete();
	}

	/**
	 * 
	 * @param predictLabel
	 *            预测标签
	 * @param path
	 *            文件位置
	 */
	public static void savePredictLabel(List<String> predictLabel, String path) {

		if (new File(path).exists()) {
			System.err.println("该文件已存在，请跟换路径或删除该路径");
			return;
		}

		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("predictHasLabel");

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		sc.parallelize(predictLabel).repartition(1).saveAsTextFile(path);
		sc.close();
	}

	// rdd 保存成 csv 文件
	public static void saveCSVFile(List<String> list, String path) throws IOException, AnalysisException {

		if (new File(path).exists()) {
			System.err.println("该文件已存在，请跟换路径或删除该路径");
			return;
		}

		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("predictHasLabel");

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<Row> javaRDD = sc.parallelize(list).repartition(1).map(new Function<String, Row>() {

			@Override
			public Row call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(arg0.split(","));
			}
		});

		List<StructField> structFields1 = new ArrayList<StructField>();
		// 列名称 列的具体类型（Integer Or String） 是否为空一般为true，实际在开发环境是通过for循环，而不是手动添加
		structFields1.add(DataTypes.createStructField("nsrid", DataTypes.StringType, true));
		structFields1.add(DataTypes.createStructField("prediction", DataTypes.StringType, true));
		structFields1.add(DataTypes.createStructField("label", DataTypes.StringType, true));
		// 构建StructType,用于最后DataFrame元数据的描述
		StructType schema = DataTypes.createStructType(structFields1);
		sqlContext.createDataFrame(javaRDD, schema).createTempView("nsrfeatureslabel");

		sqlContext.sql("select * from nsrfeatureslabel").repartition(1).write().format("com.databricks.spark.csv")
				.option("header", "true")// 在csv第一行有属性"true"，没有就是"false"
				.option("delimiter", ",")// 默认以","分割
				.save(path);

	}
}
