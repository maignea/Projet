package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Calendar;

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
import com.runmycode.mapred.Mapper;
import com.runmycode.mapred.OutputCollector;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TaskStatus;
import com.runmycode.ro.Param;

/**
 * Mapper for tests.
 * Simulation of a treatment done by a worker.
 */
public class MapperTest implements Mapper<Key, DataIndex, com.runmycode.mapred.impl.Key, Integer>
{
	
	
	
	//private QueueSender computingReportationQueueSender;
	//private QueueSession computingReportationQueueSession;

	public MapperTest() /*throws JMSException*/ {

		super();
		/*ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
		activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616");
		QueueConnection connection = activeMQConnectionFactory.createQueueConnection();
		computingReportationQueueSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
		ActiveMQQueue activeMQQueue = new ActiveMQQueue("computingReportationQueue");
		
		computingReportationQueueSender = computingReportationQueueSession.createSender(activeMQQueue);*/
	}

	@Override
	//public void map(Key key, DataIndex value, OutputCollector<com.runmycode.mapred.impl.Key, Integer> output, Reporter reporter) throws Exception
	public void map(Key key, DataIndex value, String outputPath, Reporter reporter) throws Exception
	{		
		MapperThread mapper = new MapperThread(outputPath, key, value, reporter);
		mapper.start();
		
	}



}
