package test;

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.RunningJob;
import com.runmycode.mapred.TaskReport;
import com.runmycode.mapred.TextOutputFormat;
import com.runmycode.mapred.client.JobClient_ClientSide;
import com.runmycode.mapred.impl.MapperTest;
import com.runmycode.mapred.impl.ReducerTest;
import com.runmycode.mapred.mongoDB.MongoDBInputFormat;
import com.runmycode.ro.InstanceSet;
import com.runmycode.ro.Param;


public class MainRunningJobClient
{
	public static void main(String[] argv)
	{	
		try
		{
			ApplicationContext context = new ClassPathXmlApplicationContext("applicationContextRMCClient.xml");
			Logger logger = Logger.getLogger(test.MainRunningJobClient.class);
			
			JobClient_ClientSide jobClient_ClientSide = (JobClient_ClientSide) context.getBean("jobClient_ClientSide");
			
			
			// computation demand
			JobConf job = new JobConf();
			
			// instanceSet is a description of the computation parameters: it arrives from the Companion Site
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
			
			job.setInputPath(job.getJobID().getId()+"\\Demandes");
			job.setOutputPath(job.getJobID().getId()+"\\Resultats");
			
			RunningJob runningJobClient = jobClient_ClientSide.submitJob(job);
			
			Thread.sleep(2000);
			
			String jobName = runningJobClient.getJobName();
			logger.info("jobName dans MainRunningJobClient = " + jobName);
			
			Iterator<TaskReport> reports = runningJobClient.getMapTaskReports().iterator();
			while(reports.hasNext())
			{
				logger.info("report = " + reports.next());
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
