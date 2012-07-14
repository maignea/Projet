package test;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.runmycode.mapred.server.RMI_JobClient_ServerSide;
import com.runmycode.mapred.server.ReporterServerSide;

public class MainRMI_RunningJobServer 
{
	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		try
		{ 	
			ApplicationContext context = new ClassPathXmlApplicationContext("applicationContextRMC_RMI_Server.xml");
			
			RMI_JobClient_ServerSide jobClient_ServerSide = (RMI_JobClient_ServerSide)context.getBean("jobClient_ServerSide");
			
			ReporterServerSide reporterServerSide = (ReporterServerSide) context.getBean("reporterServerSide");
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
