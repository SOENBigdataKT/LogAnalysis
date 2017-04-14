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
 * number of sessions that were started for user achille
 */
public class Question2 {

	@SuppressWarnings({ "resource", "unchecked", "rawtypes" })
	public static void main(String[] args) {
		
		String Iliad = "/home/akella/Desktop/Assignment/iliad";
		String Odyssey = "/home/akella/Desktop/Assignment/odyssey";
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");
		

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logRDDIllad = sc.textFile(Iliad);
		JavaRDD<String> logRDDOdyssey = sc.textFile(Odyssey);
	

		JavaRDD<String> sessionCount_Iliad = sessionDetaililliad(logRDDIllad, "Starting Session", "achille");
		JavaRDD<String> sessionCount_oddyssey = sessionDetaililliad(logRDDOdyssey, "Starting Session", "achille");

		// sessions that were started for user achille
		
		System.out.println("Sessions of user 'achille'\n + Iliad :" + sessionCount_Iliad.count() + "\n"
				+ "+ Odyssey :" + sessionCount_oddyssey.count());

		JavaRDD<String> sessionDetail_Iliad = sessionDetaililliad(logRDDIllad, "Starting Session", "user");
		JavaRDD<String> sessionDetail_oddyssey = sessionDetaililliad(logRDDOdyssey, "Starting Session", "user");

		
		List<String> sessionUserIliad = getUsers(sessionDetail_Iliad);
		List<String> sessionUserOddessey = getUsers(sessionDetail_oddyssey);
		
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

	/**
	* @param logRDDIllad
	* @return
	*/
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

}
