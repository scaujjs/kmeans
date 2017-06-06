package kmeans;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//the first intWritable is the cluster it belongs to,the Text is the String conveted from this points.
public  class CalculateReducer 
     extends Reducer<IntWritable,Text,Text,IntWritable> {


      int kvalues;
      int dimensions;

	  
    private IntWritable result = new IntWritable();
  
	protected void setup(Context context){
		Configuration conf =context.getConfiguration();
		Assistance.readConf(conf,this);
	} 

  
  //the reduce(0 function recalculates the centre of this cluster, and set it into configuration.
  public void reduce(IntWritable key, Iterable<Text> points, 
                     Context context
                     ) throws IOException, InterruptedException {
  	
    double[] newPoint =new double[dimensions];
    for(int i=0;i<=dimensions-1;i++){
  	  newPoint[i]=0;
    }
    
    int num=0;
    
    for (Text val : points) {
  	  
  	num++;
  	  
      double[] pointTemp=Assistance.StringToPoint(val.toString(),dimensions);
      
      for(int i=0;i<=dimensions-1;i++){
      	newPoint[i]+=pointTemp[i];
      }
      
    }
    
    for(int i=0;i<=dimensions-1;i++){
  	  newPoint[i]=newPoint[i]/num;
      }
    
    result.set(num);
    String string=Assistance.pointToString(newPoint,dimensions);
    context.write(new Text(string), result);
    Configuration conf=context.getConfiguration();
    
 
    writeInFS(key.get(),string,conf);

  }
  

  
  // update the kpoint into hdfs
  private void writeInFS(int i,String string,Configuration conf)throws IOException {
	  FileSystem fs = FileSystem.get(conf);
	  
	  String fileName="/kmeansTask/newkpoints/point"+i;
	  
	   FSDataOutputStream fops=fs.create(new Path(fileName), true);
	  fops.writeBytes(string);
	  fops.close();
	  
  }
  
  
 
}