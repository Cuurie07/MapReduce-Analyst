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
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;

/*authors:CUURIE ATHIYAMAN, PRITHVI GANESAN*/

public class popcourse {

public static class Mapper1
 extends Mapper<Object, Text, Text, IntWritable>{
 //private IntWritable one = null;
 private Text word = new Text();
 public void map(Object key, Text value, Context context
 ) throws IOException, InterruptedException {
String line = value.toString();
int count;
 try {
 
 String[] splitted = line.split(",");
 		if(splitted.length==9){
		boolean match=true;
 		if(splitted[2].toLowerCase().indexOf("arr") != 0){
 		if(!(splitted[2].matches("(?i)unknwn|UNKNOWN"))){
 		
 		StringBuilder sb= new StringBuilder();
 		sb.append(splitted[1].replaceAll("\\s",""));
 		sb.append("_");
 		sb.append(splitted[6].replaceAll("\\s",""));
 		String s=sb.toString();
		String s1=s.replaceAll("\\s","");
		word.set(s1);
 		
 		count=Integer.parseInt(splitted[7]);
 		IntWritable one=new IntWritable(count);
 		
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
 extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();
 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
 int sum = 0;
  for (IntWritable val : values) {
 sum += val.get();
 }
 result.set(sum);
 context.write(key, result);
 }
 }

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
sb.append(keys[1]);
sb.append("_");
sb.append(splitted[1]);
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
	int max=0;
	String cname="";
	for (Text val : values) {
 	tval = val.toString();
	String[] vals=tval.split("_");
	if(Integer.parseInt(vals[1])>max){
	cname=vals[0];
	max=Integer.parseInt(vals[1]);
	}
 	}
 	result.set(new Text(Integer.toString(max)));
	String myown=key.toString()+"_"+cname;
 	context.write(new Text(myown), result);
	
	
}
}

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "get total number of students for every course");
    job.setJarByClass(popcourse.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "get max");
    job2.setJarByClass(popcourse.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}




