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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Job3_TFIDF extends Configured implements Tool {

	public static final String Path_in =  "/TF-IDF/j2-output/";
	public static final String Path_out =  "/TF-IDF/output/";
	
	// The same configuration as Job_2
	@Override
	public int run(String[] arg) throws Exception {
		
		Job job3 = Job.getInstance (getConf(), "Job3_TFIDF");
		job3.setJarByClass(getClass());
		
		TextInputFormat.addInputPath(job3, new Path(Path_in));
		job3.setInputFormatClass(TextInputFormat.class);
		
		TextOutputFormat.setOutputPath(job3, new Path(Path_out));
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		job3.setMapperClass(Mapper_J3.class);
		job3.setReducerClass(Reducer_J3.class);

		job3.waitForCompletion(true);
		
		return job3.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run( new Job3_TFIDF(), args);
		System.exit(exitCode);
		
		
	}
	/* Mapper
	Input <NumLine, word#docname n/N>
	Splitting input 
	Output <word, docname=n/N>
	*/
public static class Mapper_J3 extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			String[] termDoc_n = value.toString().split("\t") ;
			String[] term_Doc = termDoc_n[0].split("#");
			context.write(new Text(term_Doc[0]) ,new Text(term_Doc[1] +"="+termDoc_n[1]) );
		}
	}
	
	/*Reducer
	Input: <word, docname=n/N>
	Output: < word#docname, TF-IDF = tf_idf TF = tf IDF = idf>
	Final step to count TF-IDF for every word in every document.
	Additionally passed NumOfDocsInCorp through configuration file. This variable calculated in bash script
	and passed as argument when job 3 is launched by script
	*/
public static class Reducer_J3 extends Reducer<Text, Text, Text, Text> {
		
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
		//Number of documents. Contains in Configuration inside context object. 
		//Passed through -Dreducer.NumF=${NUM} argument in bash launch
		int NumOfDocsInCorp = Integer.parseInt(context.getConfiguration().get("reducer.numF"));
		// How many documents contains this word
		int NumOfDocsKeyAppeared = 0;
		Map<String, String> docFreq = new HashMap<String, String>();
		
		// Iterating over values(docname=n/N).
		
		// 
		for (Text value:values){
			// On every iteration value is splitted into document name and n/N
			String[] docname_n = value.toString().split("=");
			// Incrementation of NumOfDocsKeyAppeared for specific word
			NumOfDocsKeyAppeared++;
			// Put into Map<docname, n/N>
			docFreq.put(docname_n[0], docname_n[1]);
		}
		// Iteration over statistics for every word from Map<,>
		// Writing into context < word#docname, TF-IDF = tf_idf TF = tf IDF = idf>
		for (String doc :docFreq.keySet()){
			
			String[] wordFreq_wordSum = docFreq.get(doc).split("/");
				
			double tf = Double.valueOf(wordFreq_wordSum[0])/ Double.valueOf(wordFreq_wordSum[1]);
			double idf = (double) NumOfDocsInCorp / (double) NumOfDocsKeyAppeared ;
			double tf_idf = (int) NumOfDocsInCorp == (int) NumOfDocsKeyAppeared ? tf : tf * Math.log10(idf);
			
			context.write(new Text(key +"#"+doc),new Text("TF-IDF = "+String.valueOf(tf_idf)+
					", TF="+String.valueOf(tf)+", IDF="+String.valueOf(idf) ));
				
		}
			
		}
	}		
 	
		
	
}
