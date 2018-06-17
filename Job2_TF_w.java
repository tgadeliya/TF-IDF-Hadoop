package tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


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





public class Job2_TF_w extends Configured implements Tool {

	public static final String Path_in =  "/TF-IDF/j1-output/part-*";
	public static final String Path_out =  "/TF-IDF/j2-output/";

	// Almost the same job initialization as in Job 1,
	// but in  setOutputKeyClass Text class setted
	@Override
	public int run(String[] arg) throws Exception {
		
		Job job2 = Job.getInstance (getConf(), "Job2_TF");
		job2.setJarByClass(getClass());
		
		TextInputFormat.addInputPath(job2, new Path(Path_in));
		job2.setInputFormatClass(TextInputFormat.class);
		
		TextOutputFormat.setOutputPath(job2, new Path(Path_out));
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setMapperClass(Mapper_J2.class);
		job2.setReducerClass(Reducer_J2.class);

		return job2.waitForCompletion(true) ? 0 : 1;
	}
	//Running of Job 2
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run( new Job2_TF_w(), args);
		System.exit(exitCode);
	}
	
public static class Mapper_J2 extends Mapper<LongWritable, Text, Text, Text>{


		// Input <word#docname n>
		// This mapper redefine <key,value> for Reducer
		// Output <docname word=n>
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] worddocname_n = value.toString().split("\t");
				String[] word_docname = worddocname_n[0].split("#");
				
				context.write(new Text(word_docname[1]), new Text(word_docname[0]+"="+worddocname_n[1]) );
			}	
	}
	
public static class Reducer_J2 extends Reducer<Text, Text, Text, Text> {
		/*
		 Input <docname word=n>
		 This reducer counts Term Frequency for word in document.
		 TF = n/N , where
		 n - frequency of the word in the document.
		 N - number of words in the documnt
		 Output <word#docname n/N>
		*/
		@Override
		protected void reduce(Text key , Iterable<Text> values, Context context) throws IOException, InterruptedException{
			// Initialization of N from Term frequency formula
			int wordIndocSum = 0;
			// Creting Map object for counting sum of the words in document
			Map<String, Integer> wordSumDict = new HashMap<String, Integer>(); 
			//Iterating over value(word=n) and counting words in document
			for (Text value:values){
				String[] Word_N =  value.toString().split("=");
				wordSumDict.put(Word_N[0], Integer.valueOf(Word_N[1]));
				wordIndocSum += Integer.parseInt(value.toString().split("=")[1]) ;
				
			}
			
			for (String word : wordSumDict.keySet()){
				//Writing in output <word#docname n/N>
				context.write(  new Text(word+"#"+key.toString()) , new Text( wordSumDict.get(word) +"/"+ wordIndocSum) );
			}
			
		}
	}
		
	
}
