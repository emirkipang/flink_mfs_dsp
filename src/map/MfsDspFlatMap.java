package map;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import util.Constant;



public class MfsDspFlatMap implements
		FlatMapFunction<String, Tuple3<String, String, String>> {

	/**
* 
*/
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(String in, Collector<Tuple3<String, String, String>> out)
			throws Exception {
		// TODO Auto-generated method stub
		String[] lines = in.split("\n");

		for (String line : lines) {

			String[] items = line.split("\\|", -1);
			String orderid = items[0];
			String msisdn = items[1];
			String transtime  = items[2];

			out.collect(new Tuple3<String, String, String>(orderid, msisdn, transtime));

		}

	}
}
