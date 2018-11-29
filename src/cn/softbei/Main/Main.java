package cn.softbei.Main;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import cn.softbei.Data.PredictModel;
import cn.softbei.Data.PredictModel;
import cn.softbei.Data.TestDataPreProcess;
import cn.softbei.MyUtil.MyUtils;
import scala.Tuple2;

public class Main implements Serializable {

	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException, AnalysisException {

		// 创建预测模型类
		PredictModel predictModel = new PredictModel();

		// 1.特征提取(评测文件特征已提取,若想重新提取，执行下面两行代码)
		// TestDataPreProcess testDataPreProcess = new TestDataPreProcess();
		// testDataPreProcess.run();

		// 2.预测没有标签的评测文件
		//List<String> predictNoLabelList = predictModel.predictNoLabel();
		// System.out.println(predictNoLabelList);
		// 保存预测标签到指定文件 "nsrid", "prediction"
		// MyUtils.savePredictLabel(predictNoLabelList, "D://TTT");

		// 保存Dataset<Row>到文件中，方便查看

		// 3.预测有标签的数据
		List<String> predictHasLabelList = predictModel.predictHasLabel();
		// 保存预测标签到指定文件 "nsrid", "prediction","label"
//		MyUtils.saveCSVFile(predictHasLabelList, "D://TTT3");

		// 4.重新训练
		//predictModel.reTrain();
		//List<String> predictNoLabelList = predictModel.predictNoLabel();

		// 5.测试索引为005的0类样本与所有1类数据，并获得此时的评测指标
		// System.out.println(predictModel.getTFNP("005"));

	}
}
