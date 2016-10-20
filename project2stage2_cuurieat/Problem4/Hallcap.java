import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.lang.StringBuilder.*;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import java.util.Collections;
import java.util.HashMap;

/*authors:CUURIE ATHIYAMAN,PRITHVI GANESAN*/
public class Hallcap {

public static class Mapper1
 extends Mapper<Object, Text, Text, IntWritable>{
 private Text word = new Text(); 
		
StringBuffer sb=new StringBuffer();		
 public void map(Object key, Text value, Context context
 ) throws IOException, InterruptedException {
String line = value.toString();
 try {
  String[] splitted = line.split(",");	
  if(splitted.length==9){
 		if(splitted[2].toLowerCase().indexOf("arr") != 0){
 		if(!(splitted[2].matches("(?i)unknwn|UNKNOWN"))){
		
		StringBuilder sb= new StringBuilder();
		String[] editted=splitted[2].split("\\s+");
 		sb.append(editted[0].replaceAll("\\s",""));
 		sb.append("/");
 		sb.append(splitted[1].replaceAll("\\s",""));
 		String s=sb.toString();
		String s1=s.replaceAll("\\s","");
		word.set(s1);
			
		IntWritable count=new IntWritable(Integer.parseInt(splitted[8]));

 		context.write(word, count);	
		}
		}
 		}
 		
 		}
 		
 		
 		
 		
 
 catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
 
}
}


 public static class Reducer1
 extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();
 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
int sum=0;

for (IntWritable val : values) {
sum+=val.get();
}


result.set(sum);
context.write(key, result);
 }}

public static class Mapper2
 extends Mapper<Object, Text, Text, Text>{

 private Text word = new Text();
 public void map(Object key, Text value, Context context
 ) throws IOException, InterruptedException {

String line = value.toString();
 try {

String[] splitted = line.split("\\t");
String[] keys=splitted[0].split("/");
word.set(keys[1]);
context.write(word,new Text(splitted[1]+"_"+keys[0]));

 }
 catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
 
}
}

public static class Reducer2
  extends Reducer<Text,Text,Text,Text> {

private Text result=new Text();
public void reduce(Text key, Iterable<Text> values,
                  Context context
                  ) throws IOException, InterruptedException {
	String tval="";
	int sum=0;
	for (Text val : values) {
 	tval = val.toString();
	String[] vals=tval.split("_");
	if(Integer.parseInt(vals[0])>100 &&Integer.parseInt(vals[0])<150){
	sum=sum+1;
	}
 	}	
	
 	result.set(new Text(Integer.toString(sum)));
 	context.write(key, result);	
}
}








public static void main(String[] args) throws Exception {
 
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Total Capacity per hall per year");
job.setJarByClass(Hallcap.class);
job.setMapperClass(Mapper1.class);
job.setCombinerClass(Reducer1.class);
job.setReducerClass(Reducer1.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
Configuration conf2 = new Configuration();
Job job2 = Job.getInstance(conf2, "halls in the range");
job2.setJarByClass(Hallcap.class);
job2.setMapperClass(Mapper2.class);
job2.setReducerClass(Reducer2.class);
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job2, new Path(args[1]));
FileOutputFormat.setOutputPath(job2, new Path(args[2]));
System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
 }
