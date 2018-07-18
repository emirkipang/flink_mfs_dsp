package main;

import java.util.HashMap;
import java.util.Map;

import join.MfsDspLeftJoin;
import map.MfsDspFlatMap;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;


public class Main {
	private HashMap<String, DataSet<String>> dataset_inputs = new HashMap<String, DataSet<String>>();
	private ExecutionEnvironment env;
	private int proses_paralel;
	private int sink_paralel;
	private Configuration parameter;
	private String outputPath;

	// tuples variable
	private DataSet<Tuple3<String, String, String>> source_tuples;
	private DataSet<Tuple3<String, String, String>> current_hive_tuples;
	private DataSet<Tuple1<String>> output;

	public Main(int proses_paralel, int sink_paralel, String outputPath) {
		this.env = ExecutionEnvironment.getExecutionEnvironment();
		this.parameter = new Configuration();
		this.outputPath = outputPath;

		this.proses_paralel = proses_paralel;
		this.sink_paralel = sink_paralel;
		this.env.setParallelism(this.proses_paralel);
		this.parameter.setBoolean("recursive.file.enumeration", true);

		// BasicConfigurator.configure(); //remove log warn

	}

	private String getOutputPath() {
		return this.outputPath;
	}

	private Configuration getParameter() {
		return this.parameter;
	}

	private ExecutionEnvironment getEnv() {
		return this.env;
	}

	public int getSink_paralel() {
		return this.sink_paralel;
	}

	private void setInput(HashMap<String, String> files) {

		for (Map.Entry<String, String> file : files.entrySet()) {
			dataset_inputs.put(
					file.getKey(),
					getEnv().readTextFile(file.getValue()).withParameters(
							getParameter()));
		}

	}

	public void processInput() {
		// input_tuples = dataset_inputs.get("source_upcc").flatMap(
		// new UpccFlatMap());
		source_tuples = dataset_inputs.get("source").flatMap(
				new MfsDspFlatMap());
		current_hive_tuples = dataset_inputs.get("current_hive").flatMap(
				new MfsDspFlatMap());

	}

	public void processAggregate() {
		// 1. upcc summary
		output = source_tuples.leftOuterJoin(current_hive_tuples).where(0).equalTo(0).with(new MfsDspLeftJoin());
		


	}

	public void sink() throws Exception {
		output.writeAsCsv(getOutputPath(), "\n", "|", WriteMode.OVERWRITE)
				.setParallelism(getSink_paralel());

	}

	public static void main(String[] args) throws Exception {
		// set data input
		HashMap<String, String> files = new HashMap<String, String>();

		/** prod **/
		 ParameterTool params = ParameterTool.fromArgs(args);
		
		 int proses_paralel = params.getInt("slot");
		 int sink_paralel = params.getInt("sink");
		 String source = params.get("source");
		 String current_hive = params.get("current_hive");
		 String output = params.get("output");
		
		 Main main = new Main(proses_paralel, sink_paralel, output);
		
		 files.put("source", source);
		 files.put("current_hive", current_hive);

		/** dev **/
//		int proses_paralel = 2;
//		int sink_paralel = 1;
//		String period = "1";
//
//		Main main = new Main(proses_paralel, sink_paralel, Constant.OUTPUT);
//		files.put("source", Constant.SOURCE);
//		files.put("current_hive", Constant.CURRENT_HIVE);

		/****/
		main.setInput(files);
		main.processInput();
		main.processAggregate();
		main.sink();

		try {
			main.getEnv().execute("job flink mfs dsp");
		} catch (Exception e) {
			// TODO Auto-generated catch blockF
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}
