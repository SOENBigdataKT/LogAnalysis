package LogAnalysis.Assignment2;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
/**
 * print the 5 most frequent error messages and their counts.
 */

public class Question6 {

	
	public static void main(String[] args) {
		
		/*if (args != null ){
			System.out.println(args[2]);
			
			
		}*/
        //int count=0;
		String logFileIliad = "/home/akella/Desktop/Assignment/iliad";
		String logFileOdyssey = "/home/akella/Desktop/Assignment/odyssey";
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");
		

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logRDDIllad = sc.textFile(logFileIliad);
		JavaRDD<String> logRDDOdyssey = sc.textFile(logFileOdyssey);
		
		List<String> numAs = logRDDIllad.filter(new Function<String, Boolean>() {
		      public Boolean call(String s) 
		      { return s.contains("error"); 
		      }
		    }).collect();
		
		/*@SuppressWarnings("unchecked")
		List<Tuple2<String, Integer>> numAs1 = (List<Tuple2<String, Integer>>) logRDDIllad.filter(new Function<String, Boolean>() {
		      public Boolean call(String s) 
		      { return s.contains("error"); 
		      }
		    }).mapToPair(word -> new Tuple2<>(numAs, word)).sortByKey();*/
		
		 JavaPairRDD<String, Integer> ones = ((AbstractJavaRDDLike<String, JavaRDD<String>>) numAs).mapToPair(new PairFunction<String, String, Integer>() {
		      public Tuple2<String, Integer> call(String s) {
		        return new Tuple2<String, Integer>(s, 1);
		      }
		    });
		
		System.out.println("5 most frequent error messages \n + Iliad :" + ones + "\n" + "+ Count :"
				+ numAs );
	}

}
