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

public class SemDay {

public static class Mapper1
 extends Mapper<Object, Text, Text, IntWritable>{
 private Text word = new Text();
 IntWritable one=new IntWritable(1); 
String msg;
int count;
String temp;
String ch="R";
String msg1;
String a="MTWRFSU";
		
StringBuffer sb=new StringBuffer();		
 public void map(Object key, Text value, Context context
 ) throws IOException, InterruptedException {
String line = value.toString();
 try {
  String[] splitted = line.split(",");	
  if(splitted.length==9){
 		if(splitted[2].toLowerCase().indexOf("arr") != 0){
 		if(!(splitted[2].matches("(?i)unknwn|UNKNOWN"))){
		if(splitted[3].toLowerCase().indexOf("unk")!=0){
		if(!splitted[3].equalsIgnoreCase("arr")){
		
		if(splitted[3].toLowerCase().indexOf("h")>=0){
				temp = splitted[3].substring(0, splitted[3].length()-2);;	
				msg1=temp+ch;	
				msg=msg1.trim();
				}
		else if(splitted[3].indexOf("-")>=0){
				int start=a.indexOf(splitted[3].charAt(0));
				int end=a.indexOf(splitted[3].charAt(2));
				msg=a.substring(start,end+1);
		    }
		else{
			msg=splitted[3].trim();			
			}
				
		if(msg.length()==1){
				
				context.write(new Text(splitted[1]+"_"+msg),one);
				return;
						
		 }			
		else{
		for(int l=0;l<msg.length();l++){
		
		context.write(new Text(splitted[1]+"_"+Character.toString(msg.charAt(l))),one);
		}
		return;
		}

		}
		}
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
String[] keys=splitted[0].split("_"); 
word.set(keys[0]);
StringBuilder sb= new StringBuilder();
sb.append(splitted[1]);
sb.append("_");
sb.append(keys[1]);
String s=sb.toString();
String s1=s.replaceAll("\\s","");
context.write(word,new Text(s1));
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
	ArrayList<Integer> maxa =new ArrayList<Integer>();
	HashMap<Integer,String> hm=new HashMap<Integer,String>();
	for (Text val : values) {
	tval=val.toString();
	String[] vals=tval.split("_");
	hm.put(Integer.parseInt(vals[0]),vals[1]);
	maxa.add(Integer.parseInt(vals[0]));
 	}
	Collections.sort(maxa);
	Collections.reverse(maxa);
	
	StringBuilder sb=new StringBuilder();
	
	for (int i=0;i<maxa.size();i++){
	int sc=maxa.get(i);
	sb.append(hm.get(sc));
	sb.append("_");
	sb.append(Integer.toString(sc));
	sb.append("/");		
	}
	
	String listString=sb.toString();
	String[] tops=listString.split("/");
	StringBuilder sbs=new StringBuilder();
	for(int j=0;j<3;j++){
	sbs.append(tops[j]);
	sbs.append("|");
	}
 	result.set(new Text(sbs.toString()));
 	context.write(key, result);
	
	
}
}





public static void main(String[] args) throws Exception {
 
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "day count per semester");
job.setJarByClass(SemDay.class);
job.setMapperClass(Mapper1.class);
job.setCombinerClass(Reducer1.class);
job.setReducerClass(Reducer1.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
Configuration conf2 = new Configuration();
Job job2 = Job.getInstance(conf2, "top 3 days");
job2.setJarByClass(SemDay.class);
job2.setMapperClass(Mapper2.class);
job2.setReducerClass(Reducer2.class);
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job2, new Path(args[1]));
FileOutputFormat.setOutputPath(job2, new Path(args[2]));
System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
 }
