package cn.softbei.MyUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import cn.softbei.po.NsrJXxNsridSimilarity2;
import cn.softbei.po.NsrJxxHwmc;
import scala.Tuple2;
import scala.reflect.internal.PrivateWithin;

public class NsrCallable implements Callable<Object> {
	private List<NsrJXxNsridSimilarity2> nsrJxxnsridList;
	private JavaSparkContext sc;

	public NsrCallable(List<NsrJXxNsridSimilarity2> nsrJxxnsridList, JavaSparkContext sc) {
		super();
		this.nsrJxxnsridList = nsrJxxnsridList;
		this.sc = sc;
	}

	@Override
	public Object call() throws Exception {

		List<Tuple2<String, String>> res = new ArrayList<>();
		for (NsrJXxNsridSimilarity2 nsrJXxNsridSimilarity2 : nsrJxxnsridList) {
			List<String> jxnsridList = new ArrayList<>();
			jxnsridList.addAll(nsrJXxNsridSimilarity2.getJxnsridList());

			List<String> xxnsridList = new ArrayList<>();
			xxnsridList.addAll(nsrJXxNsridSimilarity2.getXxnsridList());

			double temp1 = 0;
			if (jxnsridList.size() >= 2)
				temp1 = MyUtils.getSimilarity1(jxnsridList, sc);
			else if (jxnsridList.size() == 0)
				temp1 = 0;
			else
				temp1 = 1;

			double temp2 = 0;
			if (xxnsridList.size() >= 2)
				temp2 = MyUtils.NsrSimilarity(xxnsridList, sc);
			else if (xxnsridList.size() == 0)
				temp2 = 0;
			else
				temp2 = 1;
			res.add(new Tuple2<String, String>(
					nsrJXxNsridSimilarity2.getNsrid() + "_" + nsrJXxNsridSimilarity2.getLabel(), temp1 + "_" + temp2));
		}

		return res;
	}

}
