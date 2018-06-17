package tfidf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Job1_TF extends Configured implements Tool {

	//Input and output Path in HDFS
	// Bash script before running Job_1 copy input files from
	// local FS to HDFS input folder
	public static final String Path_in =  "/TF-IDF/input/";
	public static final String Path_out =  "/TF-IDF/j1-output/";

	//Execution of Job1_TF class
	//Running new Configuration file and new Job task
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new Job1_TF(), args);
		System.exit(exitCode);
	}

	//Function run() is used to configure job task.
	@Override
	public int run(String[] arg) throws Exception {
		// Create job1 task by passing Configuration file and name for the job
		Job job1 = Job.getInstance (getConf(), "Job1_TF");
		job1.setJarByClass(getClass());

		/*
		Add input path to job; Set input format for job1
		TextInputFormat is standard class for the input
		*/
		TextInputFormat.addInputPath(job1, new Path(Path_in));
		job1.setInputFormatClass(TextInputFormat.class);

		//Configuring Input if the job. The same as Input above
		TextOutputFormat.setOutputPath(job1, new Path(Path_out) );
		job1.setOutputFormatClass(TextOutputFormat.class);

		/*
		MapReduce paradigm uses <key,value> pairs as data streaming format
		In this job we used as output <Text,IntWritable>, because our output
		is <WORD#DOCNAME 1>
		This classes is Hadoop realisation of standard classes.
		*/
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		//Set Mapper class and Reducer class for the job
		job1.setMapperClass(Mapper_J1.class);
		job1.setReducerClass(Reducer_J1.class);

		// Return 1 if success and 0 if failure
		return job1.waitForCompletion(true) ? 0 : 1;
	}

	// Below realization of Mapper and Reducer classes with overrided map() and reduce() functions

	/*
	This mapper takes splitted text as input. Mapper splits text in <Key line>,
	where Key - line number and line - line of input text
	Output: <word#docname 1>

	*/
public static class Mapper_J1 extends Mapper<LongWritable, Text, Text, IntWritable>{

		//Creating variables using Hadoop realization of standard classes
		private final static IntWritable one =new IntWritable(1);
		private final Text word = new Text();


		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Using context we can get document name. It is used to separate words from different documents
			String docname = ((FileSplit) context.getInputSplit()).getPath().getName();

			// Tokenize line using StringTokenizer
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			//Iterating over all tokens in tokenizer
			while (tokenizer.hasMoreTokens()){
				//Initially delete all punctuation and other signs, concat with document name
				//and write to word variable that uses Hadoop Text class relization
				word.set(tokenizer.nextToken().replaceAll("[^a-zA-Z0-9]", "")+"#"+docname);
				//Output is written into context, which passed into map function with Key and Value
				context.write(word, one);
			}
		
}
	}
	/*
	In this job Reducer is used sum up all 1 for every unique word#docname in corpus
	In the result we have simple counter for the words in the particular document

	Input:<word#docname 1>, key sorted alphabetically(in Bash function sort is an equivalent for sorting)
	Output: <word#docname sum(1+1+..)>
	 */
public static class Reducer_J1 extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
											throws IOException, InterruptedException {
			// SUM variable for every key
			int sum = 0;

			//Iteration over values and summing all 1
			for (IntWritable val : values) {
				sum += val.get();
			}

			//Writing results to the context
			//.write() method writes into output key and value with \t separator
			context.write(key, new IntWritable(sum));
		}
	}

}
