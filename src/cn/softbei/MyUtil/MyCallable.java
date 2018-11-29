package cn.softbei.MyUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import cn.softbei.po.NsrJxxHwmc;
import scala.Tuple2;
import scala.reflect.internal.PrivateWithin;

public class MyCallable implements Callable<Object> {
	private List<Row> nsrJxxHwmcList;
	private JavaSparkContext sc;

	public MyCallable(List<Row> nsrJxxHwmcList, JavaSparkContext sc) {
		super();
		this.nsrJxxHwmcList = nsrJxxHwmcList;
		this.sc = sc;
	}

	@Override
	public Object call() throws Exception {

		List<Tuple2<String, String>> res = new ArrayList<>();
		for (Row row : nsrJxxHwmcList) {
			String jxhwmc = row.getAs("jxhwmc").toString();

			String xxhwmc = row.getAs("xxhwmc").toString();
			double temp = 0;
			if (!jxhwmc.equals("") && !xxhwmc.equals(""))
				temp = MyUtils.getSimilarity1(Arrays.asList(jxhwmc, xxhwmc, ""), sc);
			res.add(new Tuple2<String, String>(
					row.getAs("nsrid").toString() + "_" + row.getAs("label").toString(), temp + ""));
		}
		
			
		return res;
	}

}
