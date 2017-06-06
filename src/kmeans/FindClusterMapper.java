package kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public  class FindClusterMapper 
   extends Mapper<Object, Text,IntWritable, Text >{
	  double[][] kpoints;
	  int kvalues;
	  int dimensions;
  
protected void setup(Context context) throws IOException{
	
	Configuration conf =context.getConfiguration();
	Assistance.readConf(conf,this);
} 
  
public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {

  
  double[] point=new double[dimensions];
  
   
  String s=value.toString();
  point=Assistance.StringToPoint(s,dimensions);
  
  IntWritable indexOfK=new IntWritable(findCluster(point));
  Text PointT=new Text(Assistance.pointToString(point,dimensions));
  
  context.write(indexOfK, PointT);
}



//find each map function call, find the cluster it belongs to.
private int findCluster(double[] point){
	int indexOfNearest=0;
	double distanceMin=calculateDistance(point,0);
	for(int i=0;i<=kvalues-1;i++){
		double ditance=calculateDistance(point,i);
		if (ditance<distanceMin){
			indexOfNearest=i;
			distanceMin=ditance;
		}
	}
	return indexOfNearest;
}

//calculate the distance between a point and a centroid
private double calculateDistance(double[] point,int index){
	double distance=0;
	for(int i=0;i<=dimensions-1;i++){
		distance+=(kpoints[index][i]-point[i])*(kpoints[index][i]-point[i]);
	}		
	
	return distance;
}

}