package cn.softbei.MyUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import cn.softbei.po.NsrJXxSe2;
import scala.Tuple2;

public class ZzsfpJXxCallable implements Callable<Object> {

	private List<String> collect;
	private JavaSparkContext sc;

	public ZzsfpJXxCallable(List<String> collect, JavaSparkContext sc) {
		super();
		this.collect = collect;
		this.sc = sc;
	}

	@Override
	public Object call() throws Exception {

		List<NsrJXxSe2> res = new ArrayList<NsrJXxSe2>();

		// zzsfp处理
		JavaRDD<String> zzsfp = sc.parallelize(collect).filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.split(",")[arg0.split(",").length - 1].equals("N)");
			}
		});

		// 分离进项
		JavaPairRDD<String, NsrJXxSe2> zzsfp_jx = zzsfp.mapToPair(new PairFunction<String, String, NsrJXxSe2>() {

			@Override
			public Tuple2<String, NsrJXxSe2> call(String arg0) throws Exception {
				// gfid+kpyf
				String key = arg0.split(",")[2] + "_" + arg0.split(",")[6];
				// jxfpid xxfpid jxje xxje jxse xxse
				NsrJXxSe2 nsrJXxSe = new NsrJXxSe2(arg0.split(",")[2] + "_" + arg0.split(",")[6], arg0.split(",")[2],
						arg0.split(",")[0].substring(6), "", arg0.split(",")[3], "0", arg0.split(",")[4], "0");
				return new Tuple2<String, NsrJXxSe2>(key, nsrJXxSe);
			}
		});

		// 分离销项
		JavaPairRDD<String, NsrJXxSe2> zzsfp_xx = zzsfp.mapToPair(new PairFunction<String, String, NsrJXxSe2>() {

			@Override
			public Tuple2<String, NsrJXxSe2> call(String arg0) throws Exception {
				// xfid+kpyf
				String key = arg0.split(",")[1] + "_" + arg0.split(",")[6];
				// jxfpid xxfpid jxje xxje jxse xxse
				NsrJXxSe2 nsrJXxSe = new NsrJXxSe2(arg0.split(",")[1] + "_" + arg0.split(",")[6], arg0.split(",")[1], "",
						arg0.split(",")[0].substring(6), "0", arg0.split(",")[3], "0", arg0.split(",")[4]);
				return new Tuple2<String, NsrJXxSe2>(key, nsrJXxSe);
			}
		});

		// 进销项合并
		JavaRDD<NsrJXxSe2> nsr_jxx_yf_row = zzsfp_jx.union(zzsfp_xx)
				.reduceByKey(new Function2<NsrJXxSe2, NsrJXxSe2, NsrJXxSe2>() {

					@Override
					public NsrJXxSe2 call(NsrJXxSe2 arg0, NsrJXxSe2 arg1) throws Exception {
						NsrJXxSe2 nsrJXxSe = new NsrJXxSe2();
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

						return nsrJXxSe;
					}
				}).map(new Function<Tuple2<String, NsrJXxSe2>, NsrJXxSe2>() {

					@Override
					public NsrJXxSe2 call(Tuple2<String, NsrJXxSe2> arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0._2;
					}
				});
		return res;
	}

}
