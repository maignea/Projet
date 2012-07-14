package test;

import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobStatus;
import com.runmycode.mapred.PropertiesUtil;
import com.runmycode.mapred.RunningJob;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskReport;
import com.runmycode.mapred.TextOutputFormat;
import com.runmycode.mapred.client.RMI_JobClient_ClientSide;
import com.runmycode.mapred.impl.MapperTest;
import com.runmycode.mapred.impl.ReducerTest;
import com.runmycode.mapred.mongoDB.MongoDBInputFormat;
import com.runmycode.ro.InstanceSet;
import com.runmycode.ro.Param;


public class MainRMI_RunningJobClient
{
	public static void main(String[] argv)
	{
		try
		{
			ApplicationContext context = new ClassPathXmlApplicationContext("applicationContextRMC_RMI_Client.xml");
			RMI_JobClient_ClientSide jobClient_ClientSide = (RMI_JobClient_ClientSide) context.getBean("jobClient_ClientSide");
			Logger logger = Logger.getLogger(test.MainRMI_RunningJobClient.class);
			
			// computation demand
			JobConf job = new JobConf();
			
			// instanceSet is a description of the computation parameters: it arrives from the Companion Site
			// It is a Java version of the XML description parameters
			InstanceSet instanceSet = new InstanceSet(0);
			Param param = new Param("n", Param.ParamDim.SCALAR, Param.ParamType.INTEGER, "0:1:10", null);
			instanceSet.addParam(param);
			job.setInputParameters(instanceSet);
			
			job.setMapperClass(MapperTest.class);
			job.setReducerClass(ReducerTest.class); 	
			
			job.setInputFormat(MongoDBInputFormat.class);
			job.setOutputFormat(TextOutputFormat.class);
			
			job.setUser("utilisateur1");
			job.setJobName("script1");
			
			ApplicationContext propertiesContext = new ClassPathXmlApplicationContext("applicationContextLocalFileSystem.xml");
			PropertiesUtil propertiesUtil = (PropertiesUtil)propertiesContext.getBean("applicationProperties");
			String fileSystemRoot = propertiesUtil.getProperty("fileSystem.root");
			
			job.setInputPath(fileSystemRoot + "/" + job.getJobID().getId() + "/Demandes");
			job.setOutputPath(fileSystemRoot + "/" + job.getJobID().getId()+ "/Resultats");
			
			//job.setInputPath(job.getJobID().getId()+"\\Demandes");
			//job.setOutputPath(job.getJobID().getId()+"\\Resultats");
			
			RunningJob runningJobClient = jobClient_ClientSide.submitJob(job);
			
			Thread.sleep(2000);
			
			long launchTime = runningJobClient.getLaunchTime();
			logger.info("launchTime dans MainRunningJobClient = " + launchTime);
			
			String jobName = runningJobClient.getJobName();			
			logger.info("jobName dans MainRunningJobClient = " + jobName);

			Iterator<TaskReport> splitReports = runningJobClient.getSplitTaskReports().iterator();
			while(splitReports.hasNext())
			{
				logger.info("split report = " + splitReports.next());
			}
			
			Iterator<TaskReport> mapReports = runningJobClient.getMapTaskReports().iterator();
			while(mapReports.hasNext())
			{
				logger.info("map report = " + mapReports.next());
			}
			
			Iterator<TaskReport> reduceReports = runningJobClient.getReduceTaskReports().iterator();
			while(reduceReports.hasNext())
			{
				logger.info("reduce report = " + reduceReports.next());
			}
			
			Set<String> taskDiagnostics = runningJobClient.getTaskDiagnostics(new TaskID(job.getJobID(), "eee"));
			logger.info("taskDiagnostics dans MainRunningJobClient = " + taskDiagnostics);
			
			long startTime = runningJobClient.getStartTime();
			long finishTime = runningJobClient.getFinishTime();
			launchTime = runningJobClient.getLaunchTime();
			
			logger.info("startTime=" + startTime + ", finishTime=" + finishTime + ", launchTime=" + launchTime);
			
			Thread.sleep(5000);
			
			float mapProgress = runningJobClient.getMapProgress();
			logger.info("mapProgress dans MainRunningJobClient = "+ mapProgress);
			
			float reduceProgress = runningJobClient.getReduceProgress();
			logger.info("reduceProgress dans MainRunningJobClient = " + reduceProgress);
			
			/*Iterator<TaskCompletionEvent> taskCompletionEvents = runningJobClient.getTaskCompletionEvents(0).iterator();
			while(taskCompletionEvents.hasNext())
			{
				System.out.println("taskCompletionEvents = " + taskCompletionEvents.next());
			}*/
			
			boolean successful = runningJobClient.isSuccessful();
			logger.info("Success = " + successful);
			
			boolean complete = runningJobClient.isComplete();
			logger.info("Complete = " + complete);
			
			JobStatus jobState = runningJobClient.getJobState();

			startTime = runningJobClient.getStartTime();
			finishTime = runningJobClient.getFinishTime();
			launchTime = runningJobClient.getLaunchTime();
			logger.info("jobState dans MainRunningJobClient = " + jobState);
			
			logger.info("startTime=" + startTime + ", finishTime=" + finishTime + ", launchTime=" + launchTime);

			/*Counters counters = runningJobClient.getCounters();
			logger.info("counters dans MainRunningJobClient = " + counters);*/
			
			/*String failureInfo = runningJobClient.getFailureInfo();
			logger.info("failureInfo dans MainRunningJobClient = " + failureInfo);*/	
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}		
	}
}
