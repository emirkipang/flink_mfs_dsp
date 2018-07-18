package join;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import util.Helper;

public class MfsDspLeftJoin
		implements
		FlatJoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple1<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void join(Tuple3<String, String, String> leftElem,
			Tuple3<String, String, String> rightElem, Collector<Tuple1<String>> out)
			throws Exception {
		// TODO Auto-generated method stub
		String output = "";

		if (rightElem == null){			
			String lacci = Helper.getLacci(leftElem.f1);
			
			output = leftElem.f0 + "|" + leftElem.f1 + "|" + leftElem.f2 + "|" + lacci;
			
			out.collect(new Tuple1<String>(output));
		}
			

	}

}
