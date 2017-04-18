package LogAnalysis.Assignment2;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

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

import scala.Tuple2;

/**
 * Helper Spark API in Java --Log Analysis
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
System.out.println("Question 5");
		
		getTotalErrMessages("/home/akella/Desktop/Assignment/iliad","/home/akella/Desktop/Assignment/odyssey");
	}

	public static void createSparkContext() {
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
	}
	public static void endSparkContext() {
		sc.stop();
	}

	public static JavaPairRDD<String, String> getCommonUsers(String inputDir1, String inputDir2) {
		createSparkContext();
		String hostFolderPath1 = inputDir1;
		//String foldername = hostFolderPath1.substring(hostFolderPath1.lastIndexOf("/")+1);
		String foldername = hostFolderPath1;
		String hostName1=foldername;
		
		String hostFolderPath2 = inputDir2;
	//	String foldername2 = hostFolderPath2.substring(hostFolderPath2.lastIndexOf("/")+1);
		String foldername2 = hostFolderPath2;
		String hostName2=foldername2;
		
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
		System.out.println("Q7: users who started a session on both hosts, i.e., on exactly 2 hosts.");
		commonUsers.foreach(new VoidFunction<String>() {
			public void call(String t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("\n"+ t);
				//System.out.println("\n"+"+"+":" +t+",");
				//System.out.println( "[("  + "'"+t+"'"+","+t + "])");
			}
		});
		
		endSparkContext();
		return null;
	}
	public static JavaPairRDD<String, String> getMutuallyExclusiveUsers(String inputDir1, String inputDir2) {
		createSparkContext();
		
		String hostFolderPath1 = inputDir1;
		//String hostName1 = hostFolderPath1.substring(hostFolderPath1.lastIndexOf("/")+1);
		String hostName1 = hostFolderPath1;
		JavaRDD<String> logRDDIliad = sc.textFile(hostFolderPath1);
		host1 = new String(hostName1);
		
		String hostFolderPath2 = inputDir2;
		//String hostName2 = hostFolderPath2.substring(hostFolderPath2.lastIndexOf("/")+1);
		String hostName2 = hostFolderPath2;
		JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
		//host2 = new String(hostName2);
		
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
		//System.out.println("+"+":" + "[(");
		
		userHostPairs.foreach(new VoidFunction<Tuple2<String,String>>() {
			public void call(Tuple2 t) throws Exception {
				// TODO Auto-generated method stub
				
				System.out.println("\n"+t);
				
			}
		});
		
		//System.out.println("])");
		endSparkContext();
		return null;
	}

	public static JavaPairRDD<String, String> getAnonymysedLogs(String inputDir1, String inputDir2) {
		createSparkContext();

		
		String hostFolderPath1 = inputDir1;
		//String hostName1 = hostFolderPath1.substring(hostFolderPath1.lastIndexOf("/")+1);
		String hostName1 = hostFolderPath1;
		JavaRDD<String> logRDDIliad = sc.textFile(hostFolderPath1);
		host1 = new String(hostName1);
		
		String hostFolderPath2 = inputDir2;
		//String hostName2= hostFolderPath2.substring(hostFolderPath2.lastIndexOf("/")+1);
		String hostName2= hostFolderPath2;
		
		JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
		//host1 = new String(hostName1);
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

	public static JavaPairRDD<String, String> getTotalSessionsPerUser(String inputDir1, String inputDir2) {
		createSparkContext();
		
		String hostFolderPath1 = inputDir1;
		//String foldername = hostFolderPath1.substring(hostFolderPath1.lastIndexOf("/")+1);
		String foldername = hostFolderPath1;
		JavaRDD<String> logRDDIliad = sc.textFile(hostFolderPath1);
		host1 = new String(foldername);
		JavaRDD<String> set1LinesWithUsers = logRDDIliad.filter(new Function<String, Boolean>() {
			public Boolean call(String s) 
			{ 
				if(/*s.contains("of user")&&*/s.contains("Started Session"))
					return true;
				else
					return false;
			}
		}).distinct();
		JavaRDD<String> hostUserPairsSet1 = set1LinesWithUsers.map(
				new Function< String, String>(){

					public String call(String s) throws Exception {
						String tempStr = s;
						String[] literals = tempStr.split("of user");
						String userName = literals[literals.length-1];
						String tempString = literals[0].trim();

						return userName.replace(".", "").toString().trim()+"-"+tempString;				
					}
				}).distinct();

		JavaPairRDD<String, Integer> userSessionCount = hostUserPairsSet1.mapToPair(
				new PairFunction<String, String, Integer>(){
					public Tuple2<String, Integer> call(String s) throws Exception {
						// TODO Auto-generated method stub
						String userName = s.split("-")[0];
						return new Tuple2<String,Integer>(userName,1);
					}
				})
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(final Integer value0, final Integer value1) {
						return Integer.valueOf(value0.intValue() + value1.intValue());
					}
				});
	//	System.out.println("Anomlysed log for Odyssey");
		System.out.println("Q4: sessions per user \n ");
		System.out.println("\n"+"+"+ foldername +":");
		userSessionCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			public void call(Tuple2 t) throws Exception {
				// TODO Auto-generated method stub
				
				System.out.println( "[("  + "'"+t._1+"'"+","+t._2 + "])");
			}
		});
		//endSparkContext();
		//
			String hostFolderPath2 = inputDir2;
			//String foldername2 = hostFolderPath2.substring(hostFolderPath2.lastIndexOf("/")+1);
			String foldername2=hostFolderPath2;
			JavaRDD<String> logRDDIOdyssey = sc.textFile(hostFolderPath2);
			host2 = new String(foldername2);
			JavaRDD<String> set1LinesWithUsers2 = logRDDIOdyssey.filter(new Function<String, Boolean>() {
				public Boolean call(String s) 
				{ 
					if(/*s.contains("of user")&&*/s.contains("Started Session"))
						return true;
					else
						return false;
				}
			}).distinct();
			JavaRDD<String> hostUserPairsSet2 = set1LinesWithUsers2.map(
					new Function< String, String>(){

						public String call(String s) throws Exception {
							String tempStr = s;
							String[] literals = tempStr.split("of user");
							String userName = literals[literals.length-1];
							String tempString = literals[0].trim();

							return userName.replace(".", "").toString().trim()+"-"+tempString;				
						}
					}).distinct();

			JavaPairRDD<String, Integer> userSessionCount2 = hostUserPairsSet2.mapToPair(
					new PairFunction<String, String, Integer>(){
						public Tuple2<String, Integer> call(String s) throws Exception {
							// TODO Auto-generated method stub
							String userName = s.split("-")[0];
							return new Tuple2<String,Integer>(userName,1);
						}
					})
					.reduceByKey(new Function2<Integer, Integer, Integer>() {
						public Integer call(final Integer value0, final Integer value1) {
							return Integer.valueOf(value0.intValue() + value1.intValue());
						}
					});
		//	System.out.println("Anomlysed log for Odyssey");
		//	System.out.println("sessions per user \n ");
			System.out.println("\n"+"+"+ foldername2 +":");
			userSessionCount2.foreach(new VoidFunction<Tuple2<String,Integer>>() {
				public void call(Tuple2 t2) throws Exception {
					// TODO Auto-generated method stub
					
					System.out.println( "[("  + "'"+t2._1+"'"+","+t2._2 + "])");
				}
			});
			endSparkContext();		
		
		
		return null;
		
	
	}
	
	public static JavaPairRDD<String, String> getNumberSessionsPerUser(String inputDir1, String inputDir2) {
		
		createSparkContext();
		
		String hostFolderPath1 = inputDir1;
		//String foldername = hostFolderPath1.substring(hostFolderPath1.lastIndexOf("/")+1);
		String hostFolderPath2 = inputDir2;
	
		JavaRDD<String> logRDDIllad = sc.textFile(hostFolderPath1);
		JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
		
		JavaRDD<String> sessionCount_Iliad = sessionDetaililliad(logRDDIllad, "Starting Session", "achille");
		JavaRDD<String> sessionCount_oddyssey = sessionDetaililliad(logRDDOdyssey, "Starting Session", "achille");
		// Print Question 2
		System.out.println("Q2: Sessions of user 'achille'\n + Iliad :" + sessionCount_Iliad.count() + "\n"
				+ "+ Odyssey :" + sessionCount_oddyssey.count());

		
		endSparkContext();
		
		return null;
	
	}
	
public static JavaPairRDD<String, String> getUniqueUserNames(String inputDir1, String inputDir2) {
		
		createSparkContext();
		
		String hostFolderPath1 = inputDir1;
		
		String hostFolderPath2 = inputDir2;
	
		JavaRDD<String> logRDDIllad = sc.textFile(hostFolderPath1);
		JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
		
		JavaRDD<String> sessionDetail_Iliad = sessionDetaililliad(logRDDIllad, "Starting Session", "user");
		JavaRDD<String> sessionDetail_oddyssey = sessionDetaililliad(logRDDOdyssey, "Starting Session", "user");
		
		List<String> sessionUserIliad = getUsers(sessionDetail_Iliad);
		List<String> sessionUserOddessey = getUsers(sessionDetail_oddyssey);
		
		System.out.println("Q3: unique user names \n + Iliad :" +sessionUserIliad + "\n" + "+ Odyssey :"
				 +sessionUserOddessey );
		
		endSparkContext();
		
		return null;
	
	}


public static JavaPairRDD<String, String> getTotalErrMessages(String inputDir1, String inputDir2) {
	
	createSparkContext();
	
	String hostFolderPath1 = inputDir1;
	String hostFolderPath2 = inputDir2;

	JavaRDD<String> logRDDIllad = sc.textFile(hostFolderPath1);
	JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
	
	long numAs = logRDDIllad.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) 
	      { return s.matches("(?i:.*error.*)"); 
	      }
	    }).count();
	
	long numBs = logRDDOdyssey.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) 
	      { return s.matches("(?i:.*error.*)");  
	      }
	    }).count();
	
	System.out.println("Q5: number of errors \n + Iliad :" + numAs + "\n" + "+ Odyssey :"
			+ numBs );
	
	endSparkContext();
	
	return null;

}

public static List<String> getUsers(JavaRDD<String> sessionDetailRDD) {
	int indexOfUser;
	int endIndex;
	String userName = null;
	List<String> uniqUsers = new ArrayList<>();
	Set<String> hs = new HashSet<>();
	List<String> detail = sessionDetailRDD.collect();
	for (String s : detail) {
		indexOfUser = (s.indexOf("user"));
		endIndex = s.indexOf(".");
		userName = s.substring(indexOfUser + 5, endIndex);

		uniqUsers.add(userName);

	}
	hs.addAll(uniqUsers);
	uniqUsers.clear();
	uniqUsers.addAll(hs);
	return uniqUsers;
}


	public static JavaRDD<String> sessionDetaililliad(JavaRDD<String> logRDDIllad, String Session, String user) {
		JavaRDD<String> sessionCount = logRDDIllad.filter(new Function<String, Boolean>() {

			/**
			* 
			*/
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String sessionC) throws Exception {
				// TODO Auto-generated method stub
				return (sessionC.contains(Session)) ? true : false;
			}
		});
		JavaRDD<String> sessionCount1 = sessionCount.filter(new Function<String, Boolean>() {

			/**
			* 
			*/
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String sessionC) throws Exception {
				// TODO Auto-generated method stub
				return (sessionC.contains(user)) ? true : false;
			}
		});
		return sessionCount1;
	}
		
	public static JavaPairRDD<String, String> getTopErrorMessages(String inputDir1, String inputDir2) {
		
		createSparkContext();
		String hostFolderPath1 = inputDir1;
		//String foldername = hostFolderPath1.substring(hostFolderPath1.lastIndexOf("/")+1);
		String foldername=hostFolderPath1;
		String hostName1=foldername;
		System.out.println("Q6: 5 most frequent error messages");

		JavaRDD<String> logRDDIliad = sc.textFile(hostFolderPath1);
	//	JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
		host1 = new String(hostName1);
		JavaRDD<String> set1LinesWithUsers = logRDDIliad.filter(new Function<String, Boolean>() {
			public Boolean call(String s) 
			{
				Pattern pattern = Pattern.compile("error", 
		                Pattern.CASE_INSENSITIVE);
				 Matcher matcher = pattern.matcher(s);

				if(matcher.find()&&s.contains(host1))
					return true;
				else
					return false;
			}
		});
		JavaPairRDD<String, Integer> userSessionCount = set1LinesWithUsers.mapToPair(
				new PairFunction<String, String, Integer>(){
					public Tuple2<String, Integer> call(String s) throws Exception {
						// TODO Auto-generated method stub
						String tempStr = s.split(host1)[1].trim();
						return new Tuple2<String,Integer>(tempStr	,1);
					}
				})
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(final Integer value0, final Integer value1) {
						return Integer.valueOf(value0.intValue() + value1.intValue());
					}
				});
		
		JavaPairRDD<Integer, String> swappedPair = userSessionCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	           public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
	               return item.swap();
	           }

	        }).sortByKey(false);
		//System.out.println("Anomlysed log for Odyssey");
		System.out.println("\n"+"+"+ foldername+":");
		for (Tuple2<Integer, String> t: swappedPair.take(5)){
            System.out.println(t);                 
        }
		endSparkContext();
		
		//for inputDir2
		
		createSparkContext();
		
		String hostFolderPath2 = inputDir2;
		String foldername2=hostFolderPath2;
		//String foldername2 = hostFolderPath2.substring(hostFolderPath2.lastIndexOf("/")+1);
		String hostName2=foldername2;
		//System.out.println("Q6: 5 most frequent error messages");
		
		JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
		host2 = new String(hostName2);
		JavaRDD<String> set1LinesWithUsers2 = logRDDOdyssey.filter(new Function<String, Boolean>() {
			public Boolean call(String s) 
			{
				Pattern pattern = Pattern.compile("error", 
		                Pattern.CASE_INSENSITIVE);
				 Matcher matcher = pattern.matcher(s);

				if(matcher.find()&&s.contains(host2))
					return true;
				else
					return false;
			}
		});
		JavaPairRDD<String, Integer> userSessionCount2 = set1LinesWithUsers2.mapToPair(
				new PairFunction<String, String, Integer>(){
					public Tuple2<String, Integer> call(String s) throws Exception {
						// TODO Auto-generated method stub
						String tempStr2 = s.split(host2)[1].trim();
						return new Tuple2<String,Integer>(tempStr2	,1);
					}
				})
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(final Integer value0, final Integer value1) {
						return Integer.valueOf(value0.intValue() + value1.intValue());
					}
				});
		
		JavaPairRDD<Integer, String> swappedPair2 = userSessionCount2.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	           public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
	               return item.swap();
	           }

	        }).sortByKey(false);
		//System.out.println("Anomlysed log for Odyssey");
		System.out.println("\n"+"+"+ foldername2+":");
		for (Tuple2<Integer, String> t1: swappedPair2.take(5)){
            System.out.println(t1);                 
        }
		
	
		
		endSparkContext();
		return null;
	}

	public static JavaPairRDD<String, String> getTotalLineCount(String inputDir1, String inputDir2) {
		// TODO Auto-generated method stub
		
			createSparkContext();
			
			String hostFolderPath1 = inputDir1;
			//String foldername = hostFolderPath1.substring(hostFolderPath1.lastIndexOf("/")+1);
			String hostFolderPath2 = inputDir2;
		
			JavaRDD<String> logRDDIllad = sc.textFile(hostFolderPath1);
			JavaRDD<String> logRDDOdyssey = sc.textFile(hostFolderPath2);
			
			System.out.println("Q1: Line Counts\n + Iliad :" + logRDDIllad.count() + "\n" + "+ Odyssey :"
					+ logRDDOdyssey.count());
			
			endSparkContext();
			
			return null;
		
	}
}