package kmeans;



import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class KMeansDriver {;
    private static int kvalues;
    private static int dimensions;
	

  
  

  
  

  public static void main(String[] args)throws Exception{
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 5) {
	      System.err.println("Usage: wordcount <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    
	    
	    
	    assert(otherArgs.length==5);
	    
	    int iterationNum=Integer.parseInt(otherArgs[0]);
	    kvalues=Integer.parseInt(otherArgs[1]);
	    dimensions=Integer.parseInt(otherArgs[2]);
	    String[] stringKpoints=initialize();	 
	    
	    Path inputPath=new Path(otherArgs[3]);
	    Path outputPath=new Path(otherArgs[4]);
	    
	    FileSystem fs=FileSystem.get(conf);
	    for(int i=0;i<=kvalues-1;i++){
	    	String s="/kmeansTask/oldkpoints/point"+i;
	    	FSDataOutputStream os=fs.create(new Path(s),true);
	    	os.write(stringKpoints[i].getBytes());
	    	os.close();
	    }
	    

	   

	    
	    setConf(kvalues,conf,dimensions,stringKpoints);

	   int timesForConverge=runKmeans(iterationNum,conf,inputPath,outputPath);
	    System.out.println(timesForConverge);	
	    
	    System.exit(0);
	    
	  
	  
  }
  
  //I want store the K cluster centre ,and kvalue, and the dimensions of the data into configuration.
  private static void setConf(int kvalue,Configuration conf,int dimensions,String[] stringKpoints){
	    
	    for(int i=0;i<=kvalue-1;i++){
	    	String kpointItr="kpoint"+i;
	    	String kpointItrValue=stringKpoints[i];
	    	conf.set(kpointItr, kpointItrValue);
	    	
	    }
	    
	    conf.set("kvalue", ""+kvalue);
	    conf.set("dimensions", ""+dimensions);
  }
  

  

  private static String[] initialize(){
	  String[] a=new String[5];
	  a[0]="6.3,3.3,6.0,2.5";
	  a[1]="5.1,3.7,1.5,0.4";
	  a[3]="4.9,2.4,3.3,1.0";
	  a[4]="5.6,2.7,4.2,1.3";
	  a[2]="6.8,3.2,5.9,2.3";
	  
	  String[] stringKpoints=new String[kvalues];
	  
	  for(int i=0;i<=kvalues-1;i++){
		  stringKpoints[i]=a[i];
		  
	  }

	  
	  return stringKpoints;
  }
  
  
  private static int runKmeans (int iterationTime, Configuration conf,Path inputPath, Path outputPath)
		  throws Exception{
	  

	  int i;
	  for(i=0;i<=iterationTime-1;i++){
	      String outpath=outputPath.toString();
		  Job job=Job.getInstance(conf,"job"+i);
		  job.setJarByClass(KMeansDriver.class);
		  job.setMapperClass(FindClusterMapper.class);
		  //job.setCombinerClass(IntSumReducer.class);
		  job.setReducerClass(CalculateReducer.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
		  job.setMapOutputKeyClass(IntWritable.class);
		  job.setMapOutputValueClass(Text.class);
		  
		  
	      FileInputFormat.addInputPath(job,inputPath );
	    
	      outpath+=("/output"+i);
	      
	      
	      FileOutputFormat.setOutputPath(job,new Path(outpath));
	      if (!job.waitForCompletion(true)) {
	          throw new InterruptedException("K-Means Iteration failed processing " + i);
	        }
	      
	      if(Assistance.converge(conf,kvalues,dimensions,0.00005))
	    	  break;

	      
	      
	  }
	  
	  return i;
	  
  } 
 
  

}