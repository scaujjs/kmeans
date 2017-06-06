package kmeans;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.IOException;


public class Assistance {
    static int OLD_VERSION=0;
    static int NEW_VERSION=1;
	
	//this function will be called in the setup() functions of Mapper class. this function will initialize the cluster centre point
	protected static void readConf (Configuration conf,FindClusterMapper mapper)throws IOException{
		mapper.kvalues=Integer.parseInt(conf.get("kvalue"));
		mapper.dimensions=Integer.parseInt(conf.get("dimensions"));

		  
		  
		mapper.kpoints=readKpoint(conf,mapper.kvalues,mapper.dimensions);
	  }
	
	
	// this function will be called in the setup() of reducer class, and will read the point value from the hdfs
	protected static void readConf(Configuration conf,CalculateReducer reducer){
		reducer.kvalues=Integer.parseInt(conf.get("kvalue"));
		reducer.dimensions=Integer.parseInt(conf.get("dimensions"));
	  
	  }
	
	
	// this function will convert String like this format"1,2,3,4,5" to double[]
	protected static double[] StringToPoint(String text, int dimensions){
		  	double[] point =new double[dimensions];
		  	
		  	String line=text.toString();
			String[] tempString=line.split(",");
			
			
			for(int i=0;i<=point.length-1;i++){
				point[i]=Double.parseDouble(tempString[i]);
			}
		      return point;
		  	

		  }
		  
	// the reverse process of the function above
	protected static String pointToString(double[] point, int dimensions){
			  	String s="";
			  	for(int i=0;i<=dimensions-1;i++){
			  		s+=point[i]+",";
			  	
			  	}
			  	
			  	return s;
			  }
	
	static boolean converge(Configuration conf,int kvalues,int dimensions,double threshold)throws IOException{
		boolean result;
		
		double oldPoint[][]=readKpoint(conf,kvalues,dimensions);
		double newPoint[][]=readKpoint(conf,NEW_VERSION,kvalues,dimensions);
        double distance = 0;  
        for (int i = 0; i <=kvalues-1; ++i){  
            for (int j = 1; j <=dimensions-1 ; ++j){  
                double tmp = Math.abs(oldPoint[i][j] - newPoint[i][j]);  
                distance += Math.pow(tmp, 2);  
            }  
        } 
        
        if(distance<threshold){
        	result=true;

        	
        }
        else{
       // 	System.out.println("Point1");
        	result=false;
            FileSystem fs=FileSystem.get(conf);
            String path="/kmeansTask/oldkpoints";
            String oldName="/kmeansTask/newkpoints";
            String newName="/kmeansTask/oldkpoints";
            if(fs.delete(new Path(path),true)){
            	System.out.println("Point 1");
            }
            else{
            	System.out.println("fail!");
            }
            fs.rename(new Path(oldName), new Path(newName));
        }
 

    	System.out.println("distance: "+distance);
		return result;
	}
	
	  
	
	// read the point from hdfs
	  static double[][] readKpoint(Configuration conf,int kvalues,int dimensions)throws IOException{
		  return readKpoint(conf,OLD_VERSION,kvalues,dimensions);
	  }
	  
	  static double[][] readKpoint(Configuration conf,int version,int kvalues,int dimensions)throws IOException{
		  

		  double[][] points=new double[kvalues][dimensions];
		  

		  FileSystem fs = FileSystem.get(conf);
		  
		  String base="";
		  if(version==OLD_VERSION){
			  base="/kmeansTask/oldkpoints/point";
		  }
		  else if(version==NEW_VERSION){
			  base="/kmeansTask/newkpoints/point";
		  }
		  
		  for(int i=0;i<=kvalues-1;i++){
			  String fileName=base+i;
			 FSDataInputStream fips=fs.open(new Path(fileName));
			 
			 String stringPoint=fips.readLine();
			 fips.close();
			 points[i]=Assistance.StringToPoint(stringPoint,dimensions );
		  }
		  
		  
		  return points;
	  }
	
	
}
