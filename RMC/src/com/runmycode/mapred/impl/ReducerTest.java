package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.QueueSender;
import javax.jms.Session;

import com.runmycode.mapred.DataIndex;
import com.runmycode.mapred.OutputCollector;
import com.runmycode.mapred.Reducer;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TaskStatus;

/**
 * Reducer for tests
 * Simulation of a treatment done by a worker
 * 
 */
public class ReducerTest implements Reducer<Integer, DataIndex, String, String> 
{
	//private transient QueueSender computingReportationQueueSender;
	//private transient QueueSession computingReportationQueueSession;

	public ReducerTest() throws JMSException 
	{
		super();
		/*ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
		activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616");
		QueueConnection connection = activeMQConnectionFactory.createQueueConnection();
		computingReportationQueueSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
		ActiveMQQueue activeMQQueue = new ActiveMQQueue("computingReportationQueue");
		
		computingReportationQueueSender = computingReportationQueueSession.createSender(activeMQQueue);*/
	}

	@Override
	public void reduce(Integer key, Iterator<DataIndex> values, String outputPath, Reporter reporter) throws Exception
	{
		new ReducerThread(key, values, outputPath, reporter).start();
		/*while(values.hasNext())
		{
			DataIndex currentIndex = values.next();
			
			//treatment execution
			BufferedReader reader = new BufferedReader(new FileReader(currentIndex.getOffset()));

			String line = null;

			for(int i = 0 ; i <= currentIndex.getRow() ; i++)
			{		
				line = reader.readLine();
			}
			String setOfvalues = line.substring(line.indexOf("Values= ")+8);
			String[] valuesTab = setOfvalues.split(" ");
			String resultValue = valuesTab[currentIndex.getColumn()];

			//writing results on filesystem
			output.collect(currentIndex.getJobID().getId(), resultValue);						
		}

		reporter.getTaskStatus().setRunState(TaskStatus.State.SUCCEEDED);
		System.out.println("Send status");
		Message message = computingReportationQueueSession.createObjectMessage(reporter);
		computingReportationQueueSender.send(message);				*/
	}
}
