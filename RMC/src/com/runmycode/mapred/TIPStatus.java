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

import java.io.Serializable;

/** The states of a {@link TaskInProgress} as seen by the JobTracker.
 */

public class TIPStatus implements Serializable
{
	
	public static enum Phase implements Serializable
	{
		SPLIT,
		MAP,
		REDUCE, 
		COMBINE,
	}
	
	// what state is the task in?
	public static enum State implements Serializable
	{
		COMMIT_PENDING, 
		RUNNING, 
		SUCCEEDED, 
		KILLED, 
		FAILED;
	}

	public TIPStatus(Phase phase, State state)
	{
		super();
		this.phase = phase;
		this.state = state;
	}
	
	public TIPStatus(String phase, String state)
	{
		if (phase != null)
		{
			for (Phase p : Phase.values())
			{
				if (phase.equalsIgnoreCase(p.toString()))
				{
					this.phase = p;
				}
			}
		}
		if (state != null)
		{
			for (State s : State.values())
			{
				if (state.equalsIgnoreCase(s.toString()))
				{ 
					this.state = s;
				}
			}
		} 
	}

	private Phase phase;
	private State state;
	
	public Phase getPhase()
	{
		return phase;
	}
	
	void setPhase(Phase phase) 
	{
		this.phase = phase;
	}

	public State getState() 
	{
		return state;
	}

	void setState(State state)
	{
		this.state = state;
	}
	
	@Override
	public String toString()
	{
		return String.valueOf(state);
	}
}