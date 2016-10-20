import java.io.IOException;
//import java.util.StringTokenizer;
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

/*authors:-CUURIE ATHIYAMAN,PRITHVI GANESAN*/

public class LocDay {

public static class Mapper1
 extends Mapper<Object, Text, Text, Text>{
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

		String[] editted=splitted[2].split("\\s+");
		
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
				word.set(msg);
				context.write(new Text(splitted[1]+"_"+editted[0]),word);
				return;
						
		 }			
		else{
		for(int l=0;l<msg.length();l++){
		word.set(new Text(Character.toString(msg.charAt(l))));
		context.write(new Text(splitted[1]+"_"+editted[0]),word);
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
 extends Reducer<Text,Text,Text,Text> {
 //private IntWritable result = new IntWritable();
Text result=new Text();
 public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {


String sum="";

for (Text val : values) {
if(sum.indexOf(val.toString())<0){
sum+=val.toString();
}
}

result.set(new Text(sum.trim()));
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
String count=Integer.toString(splitted[1].length());
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
	int max=0;
	String cname="";
	for (Text val : values) {
 	tval = val.toString();
	String[] vals=tval.split("_");
	if(Integer.parseInt(vals[0])>=max){
	cname=val.toString()+"|"+cname;
	max=Integer.parseInt(vals[0]);
	}
 	}	
	
 	result.set(new Text(cname.substring(0,cname.length()-1)));
 	context.write(key, result);	
}
}









 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "get total of unique days for each location for each semester");
    job.setJarByClass(LocDay.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "get top locations");
    job2.setJarByClass(LocDay.class);
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
