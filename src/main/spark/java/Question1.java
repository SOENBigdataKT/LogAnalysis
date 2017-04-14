package LogAnalysis.Assignment2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Line Counts for the individual hosts
 */
public class Question1 {

  public static void main(String[] args) {
	  
	  SparkSession spark = SparkSession
		      .builder()
		      .appName("Line Count").master("local[*]")
		      .getOrCreate();
	  
    // Read the source file
    
    JavaRDD<String> lines = spark.read().textFile("/home/akella/Desktop/Assignment/iliad").javaRDD();
    JavaRDD<String> lines1 = spark.read().textFile("/home/akella/Desktop/Assignment/odyssey").javaRDD();
    // Gets the number of entries in the RDD
    long count = lines.count();
    long count1 = lines1.count();
    System.out.println(String.format("line counts  %s  %d",args[0],count));
    System.out.println(String.format("line counts  %s  %d",args[1],count1));
  }

}