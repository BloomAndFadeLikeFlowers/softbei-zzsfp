package cn.softbei.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import cn.softbei.MyUtil.MyUtils;
import cn.softbei.po.NsrJXxNsridSimilarity2;
import scala.Tuple2;

public class ttt implements Serializable {

	@Test
	public void remove32() {

		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("www");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Tuple2<String, String>> t1 = sc.parallelize(Arrays.asList(new Tuple2<String, String>("1", "DDD")));
		JavaRDD<Tuple2<String, String>> t2 = sc.parallelize(Arrays.asList(new Tuple2<String, String>("1", "DDD")));
		JavaRDD<Tuple2<String, String>> t3 = sc.parallelize(Arrays.asList(new Tuple2<String, String>("2", "DDD")));
		JavaRDD<Tuple2<String, String>> t4 = sc.parallelize(Arrays.asList(new Tuple2<String, String>("2", "DDD")));

		System.out.println(t1.union(t2).union(t3).union(t4).mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0;
			}
		}).reduceByKey(new Function2<String, String, String>() {
			
			@Override
			public String call(String arg0, String arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		}).collect());
		;

		// List<String> list = Arrays.asList("1", "2", "3");
		//
		// final CopyOnWriteArrayList<String> cowList = new
		// CopyOnWriteArrayList<String>(list);
		// for (String item : cowList) {
		// if (item.equals("1")) {
		// cowList.remove(item);
		// }
		// }

		// Iterator<String> iter = list.iterator();
		// while (iter.hasNext()) {
		// String item = iter.next();
		// if (item.equals("1")) {
		// iter.remove();
		// }
		// }
		// System.out.println(cowList);
	}

	@Test
	public void ttt() {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("tt");
		// conf.set("spark.driver.maxResultSize", "12g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		String path = "D://Data/softbei/Test5/nsr_jxx_nsridsimilarity_yf_label_big_object/part-00000";

		List<NsrJXxNsridSimilarity2> temp = sc.objectFile(path).map(new Function<Object, NsrJXxNsridSimilarity2>() {

			@Override
			public NsrJXxNsridSimilarity2 call(Object arg0) throws Exception {
				// TODO Auto-generated method stub
				return (NsrJXxNsridSimilarity2) arg0;
			}

		}).collect();
		for (NsrJXxNsridSimilarity2 nsrJXxNsridSimilarity2 : temp) {
			System.out.println("------------------------");
			System.out.println(nsrJXxNsridSimilarity2.getNsrid());
			System.out.println(nsrJXxNsridSimilarity2.getJxnsridList());
			System.out.println(nsrJXxNsridSimilarity2.getXxnsridList());
			System.out.println("------------------------");
		}
	}

	@Test
	public void rrr() {
		// 初始化SparkContext
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("tt");
		// conf.set("spark.driver.maxResultSize", "12g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		List<String> list = Arrays.asList("");

	}
}
