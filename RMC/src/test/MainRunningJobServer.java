package test;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.runmycode.mapred.server.JobClient_ServerSide;
import com.runmycode.mapred.server.ReporterServerSide;
 
public class MainRunningJobServer 
{
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		try
		{ 
			ApplicationContext context = new ClassPathXmlApplicationContext("applicationContextRMCServer.xml");
			
			JobClient_ServerSide jobClient_ServerSide = (JobClient_ServerSide)context.getBean("jobClient_ServerSide");
			
			ReporterServerSide reporterServerSide = (ReporterServerSide) context.getBean("reporterServerSide");	
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
