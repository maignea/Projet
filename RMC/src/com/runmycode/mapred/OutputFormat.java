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
import java.io.Serializable;

/** 
 * <code>OutputFormat</code> describes the output-specification for a 
 * Map-Reduce job.
 *
 * <p>The Map-Reduce framework relies on the <code>OutputFormat</code> of the
 * job to:<p>
 * <ol>
 *   <li>
 *   Validate the output-specification of the job. For e.g. check that the 
 *   output directory doesn't already exist. 
 *   <li>
 *   Provide the {@link RecordWriter} implementation to be used to write out
 *   the output files of the job. Output files are stored in a 
 *   {@link FileSystem}.
 *   </li>
 * </ol>
 * 
 * @see RecordWriter
 * @see JobConf
 */
public interface OutputFormat<K, V> extends Serializable
{
	/** 
	 * Get the {@link RecordWriter} for the given job.
	 *
	 * @param job configuration for the job whose output is being written.
	 * @param name the unique name for this part of the output.
	 * @param progress mechanism for reporting progress while writing to file.
	 * @return a {@link RecordWriter} to write the output for the job.
	 * @throws IOException
	 */
	 RecordWriter<K, V> getRecordWriter(JobConf job, String name, Progressable progress) throws IOException;  
}

