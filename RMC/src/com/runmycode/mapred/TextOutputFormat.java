/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.runmycode.mapred;

import java.io.IOException;

/** An {@link OutputFormat} that writes plain text files. 
 */
public class TextOutputFormat<K, V> extends FileOutputFormat<K, V>
{
	protected static class LineRecordWriter<K, V> implements RecordWriter<K, V>
	{
		/**
		 * Write the object to the byte stream, handling Text as a special
		 * case.
		 * @param o the object to print
		 * @throws IOException if the write throws, we pass it on
		 */
		private void writeObject(Object o) throws IOException
		{
			
		}

		public synchronized void write(K key, V value) throws IOException
		{
			
		}

		public synchronized void close(Reporter reporter) throws IOException
		{
			
		}
	}
	
	@Override
	public RecordWriter<K, V> getRecordWriter(JobConf job, String name, Progressable progress) throws IOException
	{
		return null;
	}	
}

