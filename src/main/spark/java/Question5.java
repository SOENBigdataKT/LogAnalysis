package LogAnalysis.Assignment2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
/**
 * the lines that contain string “error”
 */
public class Question5 {

	
	public static void main(String[] args) {
		
		/*if (args != null ){
			System.out.println(args[2]);
			
			
		}*/

		String logFileIliad = "/home/akella/Desktop/Assignment/iliad";
		String logFileOdyssey = "/home/akella/Desktop/Assignment/odyssey";
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");
		

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logRDDIllad = sc.textFile(logFileIliad);
		JavaRDD<String> logRDDOdyssey = sc.textFile(logFileOdyssey);
		
		
		long numAs = logRDDIllad.filter(new Function<String, Boolean>() {
		      public Boolean call(String s) 
		      { return s.contains("error"); 
		      }
		    }).count();
		
		 
		
		
		long numBs = logRDDOdyssey.filter(new Function<String, Boolean>() {
		      public Boolean call(String s) 
		      { return s.contains("error"); 
		      }
		    }).count();
		
		System.out.println("number of errors \n + Iliad :" + numAs + "\n" + "+ Odyssey :"
				+ numBs );
	}

}
