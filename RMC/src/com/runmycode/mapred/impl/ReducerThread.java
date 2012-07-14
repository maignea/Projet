package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Iterator;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueConnection;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import com.runmycode.mapred.DataIndex;
import com.runmycode.mapred.Key;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TaskStatus;
import com.runmycode.ro.Param;

public class ReducerThread extends Thread{
	
	private String outputPath;
	private Integer key;
	private Iterator<DataIndex> values;
	private Reporter reporter;
	private QueueSession computingReportationQueueSession;
	private QueueSender computingReportationQueueSender;
	
	public ReducerThread(Integer key, Iterator<DataIndex> values, String outputPath, Reporter reporter) throws JMSException {
		this.outputPath = outputPath;
		this.key = key;
		this.values = values;
		this.reporter = reporter;
		ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
		activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616");
		QueueConnection connection = activeMQConnectionFactory.createQueueConnection();
		computingReportationQueueSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
		ActiveMQQueue activeMQQueue = new ActiveMQQueue("computingReportationQueue");
		
		computingReportationQueueSender = computingReportationQueueSession.createSender(activeMQQueue);
	}

	public void run()
	{
		try
		{
			ReducerCollector output = new ReducerCollector(outputPath);
			output.open();
			
			while(values.hasNext())
			{
				DataIndex currentIndex = values.next();
				
				//treatment execution
				BufferedReader reader = new BufferedReader(new FileReader(new File(new URI(currentIndex.getOffset()))));

				String line = null;

				for(int i = 0 ; i <= currentIndex.getRow() ; i++)
				{		
					line = reader.readLine();
				}
				String setOfvalues = line.substring(line.indexOf("Values=")+8);
				String[] valuesTab = setOfvalues.split(" ");
				String resultValue = valuesTab[currentIndex.getColumn()];
				
				reader.close();

				//writing results on filesystem
				output.collect(currentIndex.getJobID().getId(), resultValue);						
			}
			reporter.getTaskStatus().setRunState(TaskStatus.State.SUCCEEDED);
			System.out.println("Send status");
			Message message = computingReportationQueueSession.createObjectMessage(reporter);
			computingReportationQueueSender.send(message);				
			
			output.close();	
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}