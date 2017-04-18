# LogAnalysis

Team Members: Tushar Gupta, V.J.R. Karthik Akella

Assignment: Log Analysis on system messages

The Log Analysis is implemented using Spark API.

Prerequisites:

1. For Executing the spark API, the following environment should be set

Java version : JDK 1.8 

Spark version : 2.11 


2. Libraries required : spark jars required to run the SPARK java class to be included in the environment path variable.

3. The input system log(for example :iilad and odyssey) folders should be placed in the same folder as of scripts.
4. Java_HOME and spark_HOME should be set in the evironment.


Install and Run:

1.  Download the source code from github: https://github.com/SOENBigdataKT/LogAnalysis into a folder on unix machine
2.  Copy the input system log(iilad and odyssey) folders into the same unix folder as in step1
3.  Firstly compile the java program (LabAssignment2Helper.java)

    javac LabAssignment2Helper.java

4.  Provide the shell script (log_analyzer.sh) the necessary execute permissions.

5.  Execute the shell script as follows

    ./log_analyzer -q  dir1 dir2


Classification of Source files:

LabAssignment2Helper.java -- Actual Spark API Java Class implementing all the 9 assignment questions
log_analyzer -- Shell script which passes input to the main class(LabAssignment2Helper.java)


