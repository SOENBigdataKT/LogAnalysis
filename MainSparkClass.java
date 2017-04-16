#package LogAnalysis.src.main.spark.java;
import LogAnalysis.src.main.spark.java.LabAssignment2Helper;

/**
 * Main Spark program in Java which is a driver to all 9 questions
 */
public class MainSparkClass {

	
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		int questionNumber=0;
		String inputDir1;
		String inputDir2;
		  try {
	            
			  questionNumber = Integer.parseInt(args[0]);
			  inputDir1 = args[1];
			  inputDir2 = args[2];
			 
			  //getTotalLineCount
			  
			  if(questionNumber == 1){
				 
				  new LabAssignment2Helper();
				  LabAssignment2Helper.getTotalLineCount(inputDir1, inputDir2);
				
			  }
			  
			  //getNumberSessionsPerUser
			  
			  else if(questionNumber == 2){
					 
				  new LabAssignment2Helper();
				  
				  LabAssignment2Helper.getNumberSessionsPerUser(inputDir1, inputDir2);
				
			  }
			  
			  //getUniqueUserNames
			  else if(questionNumber == 3){
					 
				  new LabAssignment2Helper();
				  
				  LabAssignment2Helper.getUniqueUserNames(inputDir1, inputDir2);
				
			  }
			  
	          //getTotalSessionsPerUser
			  else if(questionNumber == 4){
				 
				  new LabAssignment2Helper();
				  
				  LabAssignment2Helper.getTotalSessionsPerUser(inputDir1, inputDir2);
				
			  }
			  
			  //getTotalErrMessages
			  else if(questionNumber == 5){
					 
				  new LabAssignment2Helper();
				  
				  LabAssignment2Helper.getTotalErrMessages(inputDir1, inputDir2);
				
			  }
			  
			  //getTopErrorMessages
			  
			  else if(questionNumber == 6){
					 
				  new LabAssignment2Helper();
				  LabAssignment2Helper.getTopErrorMessages(inputDir1, inputDir2);
				
			  }
			  
			  //getCommonUsers
			  else if(questionNumber == 7){
					 
				  new LabAssignment2Helper();
				  LabAssignment2Helper.getCommonUsers(inputDir1, inputDir2);
				
			  }
			  
			  //exactly 1 host logs
			  else if(questionNumber == 8){
					 
				  new LabAssignment2Helper();
				  LabAssignment2Helper.getMutuallyExclusiveUsers(inputDir1, inputDir2);
				
			  }
			  
			  //getAnonymysedLogs
			  else if(questionNumber == 9){
				  new LabAssignment2Helper(); 
					
				  LabAssignment2Helper.getAnonymysedLogs(inputDir1, inputDir2);
				
			  }
				  
	        }
	        catch (NumberFormatException nfe) {
	       
	            System.out.println("The first argument must be an integer.");
	            System.exit(1);
	        }

	}

}
