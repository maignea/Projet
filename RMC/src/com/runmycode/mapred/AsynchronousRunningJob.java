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

/** 
 * <code>RunningJob</code> is the user-interface to query for details on a 
 * running Map-Reduce job.
 * 
 * <p>Clients can get hold of <code>RunningJob</code> via the {@link JobClient}
 * and then query the running-job for details such as name, configuration, 
 * progress etc.</p> 
 * 
 * @see JobClient
 */
public interface AsynchronousRunningJob 
{
	public void setMapProgress(float progress, JobID jobID);

}
