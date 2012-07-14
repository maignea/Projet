package com.runmycode.mapred.mongoDB;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.runmycode.mapred.DataIndex;
import com.runmycode.mapred.InputFormat;
import com.runmycode.mapred.InputSplit;
import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.Key;
import com.runmycode.mapred.RecordReader;
import com.runmycode.mapred.Reporter;

public class MongoDBInputFormat implements Serializable, InputFormat<Key,String>
{
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	
	private transient HashMap<String, List<DataIndex>> offsetTable = new HashMap<String, List<DataIndex>>();

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException 
	{
		String jobID = job.getJobID().getId();

		List<DataIndex> dataIndexes = mongoOperations.find(Query.query(Criteria.where("jobID._id").is(jobID).and("isMap").is(true)), DataIndex.class);
		
		for(DataIndex dataIndex : dataIndexes)
		{
			if(offsetTable.isEmpty() == true)
			{
				List<DataIndex> indexes = mongoOperations.find(Query.query(Criteria.where("offset").is(dataIndex.getOffset()).and("isMap").is(true)), DataIndex.class);
				
				offsetTable.put(dataIndex.getOffset(), indexes);
			}
			else
			{
				if(offsetTable.containsKey((dataIndex.getOffset())) == false)
				{
					List<DataIndex> indexes = mongoOperations.find(Query.query(Criteria.where("offset").is(dataIndex.getOffset()).and("isMap").is(true)), DataIndex.class);
				
					offsetTable.put(dataIndex.getOffset(), indexes);
				}
			}
		}
		
		Set<String> keys = offsetTable.keySet();
		
		List<InputSplit> inputSplits = new ArrayList<InputSplit>();
		
		for(String key : keys)
		{
			InputSplit inputSplit = new InputSplitImpl(key);
			inputSplits.add(inputSplit);
		}
		return inputSplits.toArray(new InputSplit[inputSplits.size()]);		
	}
	
	class InputSplitImpl implements InputSplit
	{
		private String key;
		
		public InputSplitImpl(String key)
		{
			this.key = key;
		}

		@Override
		public long getLength() throws IOException
		{
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException
		{
			return null;
		}

		public String getKey()
		{
			return key;
		}
	}
	
	class RecordReaderImpl implements RecordReader<Key, DataIndex>
	{	
		private List<DataIndex> datas;
		private int indexOfRecordReader;
	
		public RecordReaderImpl(List<DataIndex> datas) 
		{
			super();
			this.datas = datas;
		}

		@Override
		public boolean next(Key key, DataIndex value) throws IOException 
		{
			if(indexOfRecordReader == datas.size())
			{
				return false;
			}
			try
			{
				key.setValue(indexOfRecordReader);
				DataIndex index = datas.get(key.getValue());
				value.setLength(index.getLength());
				value.setOffset(index.getOffset());
				value.setRow(index.getRow());
				value.setJobID(index.getJobID());
				value.setMap(index.isMap());
				indexOfRecordReader++;
				return true;
			}
			catch(Exception e)
			{
				throw new IOException("key error !");
			}			
		}

		@Override
		public Key createKey() 
		{
			Key i = new Key(indexOfRecordReader);
			return i;
		}

		@Override
		public DataIndex createValue() 
		{
			return new DataIndex();
		}

		@Override
		public long getPos() throws IOException 
		{
			return 0;
		}

		@Override
		public void close() throws IOException 
		{

		}

		@Override
		public float getProgress() throws IOException 
		{
			return 0;
		}		
	}

	@Override
	public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException 
	{		
		if(split instanceof InputSplitImpl)
		{
			InputSplitImpl inputSlit = (InputSplitImpl)split;
			String key = inputSlit.getKey();
			List<DataIndex> datas = offsetTable.get(key);
			return new RecordReaderImpl(datas);
		}
		throw new IOException("Split is not an InputSplitImpl ! Use this.getSplits");
	}
}
