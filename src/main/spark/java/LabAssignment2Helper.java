package LogAnalysis.Assignment2;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.file.SyncableFileOutputStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;
/**
 * Combining logs and Anonymization
 */
public class LabAssignment2Helper {

	static Set<String> hostUsers;
	static String host1;
	static String host2;
	static int count1=0;
	static int count2=0;
	static JavaSparkContext sc;
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		getCommonUsers();
		getMutuallyExclusiveUsers();
		anomlysedLogs();
	}

	public static void createSparkContext() {
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
	}
	public static void endSparkContext() {
		sc.stop();
	}

	public static JavaPairRDD<String, String> getCommonUsers() {
		createSparkContext();
		String hostName1="iliad";
		String hostName2="odyssey";
		String hostFolderPath1 = "/home/tushargupta98/Documents/BigData/LabAssignment2/iliad";
		String hostFolderPath2 = "/home/tushargupta98/Documents/BigData/LabAssignment2/odyssey";
		JavaRDD<String> logRDDIliad = sc.textFile(hostFolderPath1);
		JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
		host1 = new String(hostName1);
		JavaRDD<String> set1LinesWithUsers = logRDDIliad.filter(new Function<String, Boolean>() {
			public Boolean call(String s) 
			{ 
				return s.contains("of user"); 
			}
		}).distinct();
		JavaPairRDD<String,String> hostUserPairsSet1 = set1LinesWithUsers.mapToPair(
				new PairFunction<String, String, String>(){

					public Tuple2<String, String> call(String s) throws Exception {
						String tempStr = s;
						String[] literals = tempStr.split("user");
						String userName = literals[literals.length-1];
						return new Tuple2<String,String>(userName.replace(".", "").toString().trim(),host1);				
					}

				}).distinct();

		host2 = new String(hostName2);
		JavaRDD<String> set2LinesWithUsers = logRDDOdyssey.filter(new Function<String, Boolean>() {
			public Boolean call(String s) 
			{ 
				return s.contains("of user"); 
			}
		}).distinct();
		JavaPairRDD<String,String> hostUserPairsSet2 = set2LinesWithUsers.mapToPair(
				new PairFunction<String, String, String>(){
					public Tuple2<String, String> call(String s) throws Exception {
						String tempStr = s;
						String[] literals = tempStr.split("user");
						String userName = literals[literals.length-1];
						return new Tuple2<String,String>(userName.replace(".", "").toString().trim(),host2);				
					}

				}).distinct();
		JavaPairRDD<String, String> hostUserUnion = hostUserPairsSet1.union(hostUserPairsSet2);
		JavaRDD<String> userNames = hostUserUnion.map(
				new Function<Tuple2<String, String>, String>(){

					public String call(Tuple2<String, String> v1) throws Exception {
						// TODO Auto-generated method stub
						return v1._1;
					}

				});
		JavaPairRDD<String, Integer> userCount = userNames.mapToPair(
				new PairFunction<String, String, Integer>(){
					public Tuple2<String, Integer> call(String s) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String,Integer>(s,1);
					}
				})
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(final Integer value0, final Integer value1) {
						return Integer.valueOf(value0.intValue() + value1.intValue());
					}
				});

		JavaRDD<String> commonUsers = userCount.filter(new Function<Tuple2<String,Integer>,Boolean>() {

			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				// TODO Auto-generated method stub
				if(v1._2==2)
					return true;
				else
					return false;
			}

		}).map(new Function<Tuple2<String,Integer>,String>(){

			public String call(Tuple2<String, Integer> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1._1;
			}
		});
		System.out.println("Users with session on exactly TWO hosts");
		commonUsers.foreach(new VoidFunction<String>() {
			public void call(String t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t);
			}
		});
		endSparkContext();
		return null;
	}
	public static JavaPairRDD<String, String> getMutuallyExclusiveUsers() {
		createSparkContext();
		String hostName1="iliad";
		String hostName2="odyssey";
		String hostFolderPath1 = "/home/tushargupta98/Documents/BigData/LabAssignment2/iliad";
		String hostFolderPath2 = "/home/tushargupta98/Documents/BigData/LabAssignment2/odyssey";
		JavaRDD<String> logRDDIliad = sc.textFile(hostFolderPath1);
		JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
		host1 = new String(hostName1);
		JavaRDD<String> set1LinesWithUsers = logRDDIliad.filter(new Function<String, Boolean>() {
			public Boolean call(String s) 
			{ 
				return s.contains("of user"); 
			}
		}).distinct();
		JavaPairRDD<String,String> hostUserPairsSet1 = set1LinesWithUsers.mapToPair(
				new PairFunction<String, String, String>(){

					public Tuple2<String, String> call(String s) throws Exception {
						String tempStr = s;
						String[] literals = tempStr.split("user");
						String userName = literals[literals.length-1];
						return new Tuple2<String,String>(userName.replace(".", "").toString().trim(),host1);				
					}

				}).distinct();

		host2 = new String(hostName2);
		JavaRDD<String> set2LinesWithUsers = logRDDOdyssey.filter(new Function<String, Boolean>() {
			public Boolean call(String s) 
			{ 
				return s.contains("of user"); 
			}
		}).distinct();
		JavaPairRDD<String,String> hostUserPairsSet2 = set2LinesWithUsers.mapToPair(
				new PairFunction<String, String, String>(){
					public Tuple2<String, String> call(String s) throws Exception {
						String tempStr = s;
						String[] literals = tempStr.split("user");
						String userName = literals[literals.length-1];
						return new Tuple2<String,String>(userName.replace(".", "").toString().trim(),host2);				
					}

				}).distinct();
		JavaPairRDD<String, String> hostUserUnion = hostUserPairsSet1.union(hostUserPairsSet2);
		JavaRDD<String> userNames = hostUserUnion.map(
				new Function<Tuple2<String, String>, String>(){

					public String call(Tuple2<String, String> v1) throws Exception {
						// TODO Auto-generated method stub
						return v1._1;
					}

				});
		JavaPairRDD<String, Integer> userCount = userNames.mapToPair(
				new PairFunction<String, String, Integer>(){
					public Tuple2<String, Integer> call(String s) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String,Integer>(s,1);
					}
				})
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(final Integer value0, final Integer value1) {
						return Integer.valueOf(value0.intValue() + value1.intValue());
					}
				});

		JavaRDD<String> mutExclusiveUsers = userCount.filter(new Function<Tuple2<String,Integer>,Boolean>() {

			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				// TODO Auto-generated method stub
				if(v1._2==1)
					return true;
				else
					return false;
			}

		}).map(new Function<Tuple2<String,Integer>,String>(){

			public String call(Tuple2<String, Integer> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1._1;
			}
		});
		
		JavaRDD<Tuple2<String, String>> userHostPairs = mutExclusiveUsers.mapToPair(
				new PairFunction<String, String,String>() {
					public Tuple2<String,String> call(String t) throws Exception {
						return new Tuple2<String,String>(t,t);
					}
				}).join(hostUserUnion).values();
		System.out.println("Users with session on only ONE host");
		userHostPairs.foreach(new VoidFunction<Tuple2<String,String>>() {
			public void call(Tuple2 t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t);
			}
		});
		endSparkContext();
		return null;
	}
	
	public static JavaPairRDD<String, String> anomlysedLogs() {
		createSparkContext();
		String hostName1="iliad";
		String hostName2="odyssey";
		String hostFolderPath1 = "/home/tushargupta98/Documents/BigData/LabAssignment2/iliad";
		String hostFolderPath2 = "/home/tushargupta98/Documents/BigData/LabAssignment2/odyssey";
		JavaRDD<String> logRDDIliad = sc.textFile(hostFolderPath1);
		JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
		host1 = new String(hostName1);
		JavaRDD<String> set1LinesWithUsers = logRDDIliad.filter(new Function<String, Boolean>() {
			public Boolean call(String s) 
			{ 
				return s.contains("of user"); 
			}
		}).distinct();
		JavaRDD<String> hostUserPairsSet1 = set1LinesWithUsers.map(
				new Function<String, String>(){

					public String call(String s) throws Exception {
						String tempStr = s;
						String[] literals = tempStr.split("user");
						String userName = literals[literals.length-1];
						return userName;				
					}

				}).distinct();
		JavaPairRDD<String,String> hostUserPairsSetDistinct1 = hostUserPairsSet1.mapToPair(
				new PairFunction<String, String, String>(){

					public Tuple2<String, String> call(String s) throws Exception {
						String tempStr = s;
						String[] literals = tempStr.split("user");
						String userName = literals[literals.length-1];
						Tuple2<String,String> tupleObj = new Tuple2<String,String>(userName.replace(".", "").toString().trim(),"user-"+count1);	
						count1++;
						return tupleObj;
					}

				});

		host2 = new String(hostName2);
		JavaRDD<String> set2LinesWithUsers = logRDDOdyssey.filter(new Function<String, Boolean>() {
			public Boolean call(String s) 
			{ 
				return s.contains("of user"); 
			}
		}).distinct();
		JavaRDD<String> hostUserPairsSet2 = set2LinesWithUsers.map(
				new Function<String, String>(){

					public String call(String s) throws Exception {
						String tempStr = s;
						String[] literals = tempStr.split("user");
						String userName = literals[literals.length-1];
						return userName;				
					}

				}).distinct();
		JavaPairRDD<String,String> hostUserPairsSetDistinct2 = hostUserPairsSet2.mapToPair(
				new PairFunction<String, String, String>(){
					public Tuple2<String, String> call(String s) throws Exception {
						String tempStr = s;
						String[] literals = tempStr.split("user");
						String userName = literals[literals.length-1];
						Tuple2<String,String> tupleObj = new Tuple2<String,String>(userName.replace(".", "").toString().trim(),"user-"+count2);	
						count2++;
						return tupleObj;				
					}

				}).distinct();
		System.out.println("Anomlysed log for Iliad");
		hostUserPairsSetDistinct1.foreach(new VoidFunction<Tuple2<String,String>>() {
			public void call(Tuple2 t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t);
			}
		});
		System.out.println("Anomlysed log for Odyssey");
		hostUserPairsSetDistinct2.foreach(new VoidFunction<Tuple2<String,String>>() {
			public void call(Tuple2 t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t);
			}
		});
		endSparkContext();
		return null;
	}
}