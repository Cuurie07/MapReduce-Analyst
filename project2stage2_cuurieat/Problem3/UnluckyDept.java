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
import java.util.Iterator;

/*authors:CUURIE ATHIYAMAN,PRITHVI GANESAN*/
public class UnluckyDept {

public static class Mapper1
 extends Mapper<Object, Text, Text, Text>{
 private Text word = new Text();
StringBuffer sb=new StringBuffer();		
 public void map(Object key, Text value, Context context
 ) throws IOException, InterruptedException {
String line = value.toString();
 try {
  String[] splitted = line.split(",");	
  if(splitted.length==11){
 		if(!(splitted[5].toLowerCase().indexOf("arr") >= 0)){
 		if(!(splitted[5].matches("(?i)unknwn|UNKNOWN"))){
		
		StringBuilder sb= new StringBuilder();
		sb.append(splitted[3]);
 		sb.append("_");
 		sb.append(splitted[4].replaceAll("\\s",""));
 		String s=sb.toString();
		String s1=s.replaceAll("\\s","");
		
		word.set(s1);		
		Text one=new Text(splitted[5]);

 		context.write(word, one);	

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
 extends Reducer<Text,Text,Text,Text> {
 //private IntWritable result = new IntWritable();
Text result=new Text();
 public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

StringBuilder sum=new StringBuilder();


for (Text val : values) {
if(sum.indexOf(val.toString())<0){
sum.append(val.toString().trim());
sum.append("/");
}
}


result.set(new Text(sum.toString().trim()));
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
String[] keys=splitted[0].split("_");
String[] vals=splitted[1].trim().split("/");
String count=Integer.toString(vals.length);
word.set(keys[0]);
context.write(word,new Text(count+"_"+keys[1]));
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
	int min=1000000;
	String dept="";
	for (Text val : values) {
 	tval = val.toString();
	String[] vals=tval.split("_");
	if(Integer.parseInt(vals[0])<=min){
	dept=val.toString()+"|"+dept;
	min=Integer.parseInt(vals[0]);
	}
 	}	

 	result.set(new Text(dept.substring(0,dept.length()-1)));
 	context.write(key, result);
}
}




public static void main(String[] args) throws Exception {
 
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "room count for each department per semester");
job.setJarByClass(UnluckyDept.class);
job.setMapperClass(Mapper1.class);
job.setCombinerClass(Reducer1.class);
job.setReducerClass(Reducer1.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
Configuration conf2 = new Configuration();
Job job2 = Job.getInstance(conf2, "get unlucky depts");
job2.setJarByClass(UnluckyDept.class);
job2.setMapperClass(Mapper2.class);
job2.setCombinerClass(Reducer2.class);
job2.setReducerClass(Reducer2.class);
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job2, new Path(args[1]));
FileOutputFormat.setOutputPath(job2, new Path(args[2]));
System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
 }
