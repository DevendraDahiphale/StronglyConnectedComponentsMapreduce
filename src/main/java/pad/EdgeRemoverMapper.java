/**
 *	@file EdgeRemoverMapper.java
 *	@brief Mapper task of the \see StarDriver Job.
 *  @author Federico Conte (draxent)
 *  
 *	Copyright 2015 Federico Conte
 *	https://github.com/Draxent/ConnectedComponents
 * 
 *	Licensed under the Apache License, Version 2.0 (the "License"); 
 *	you may not use this file except in compliance with the License. 
 *	You may obtain a copy of the License at 
 * 
 *	http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *	Unless required by applicable law or agreed to in writing, software 
 *	distributed under the License is distributed on an "AS IS" BASIS, 
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 *	See the License for the specific language governing permissions and 
 *	limitations under the License. 
 */

package pad;

import java.io.IOException;
//import java.lang.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/** Mapper task of the \see StarDriver Job. */
public class EdgeRemoverMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> 
{
//	private boolean smallStar;
	private int edgeNumber;
	private int edgeCounter;
	private IntWritable nodeID = new IntWritable();
	private NodesPairWritable pair = new NodesPairWritable();
	public static final IntWritable MINUS_ONE = new IntWritable( -1 );

	/**
	* Setup method of the this StarMapper class.
	* Extract the <em>type</em> variable from the context configuration.
	* Based on this value, this Mapper will behave as a Small-Star Mapper or Large-Star Mapper.
	* @param context	context of this Job.
	*/
	public void setup( Context context )
	{
		//smallStar = context.getConfiguration().get( "type" ).equals( "SMALL" );
		edgeNumber = Integer.parseInt(context.getConfiguration().get( "edgeID" ));
		edgeCounter = 0;
	}
	
	/**
	* Map method of the this StarMapper class.
	* If it is a Large-Star Mapper, it emits the pairs <u,v> and <v,u>.
	* If it is a Small-Star Mapper, it emits the pair <max(u,v), min(u,v)>.
	* @param nodeID			identifier of the node.
	* @param neighbourID		identifier of the neighbour.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void map(IntWritable nodeID, IntWritable neighbourID, Context context ) throws IOException, InterruptedException 
	{
		if(edgeNumber == context.getCounter(UtilCounters.NUM_EDGE_COUNTER).getValue() && neighbourID.get() != -1) {
			System.out.println("Skipping Edge " + edgeCounter);
			context.getCounter( UtilCounters.NUM_EDGE_COUNTER ).increment( 1 );
			return;		
		}

	//	System.out.println("in map of edge remover, edgenumber " + edgeNumber + "edgeCOunter " + edgeCounter);

		//pair.NodeID = nodeID.get();
		//pair.NeighbourID =  neighbourID.get();
//		this.nodeID.set( nodeID );		
		context.write( nodeID, neighbourID );
		context.getCounter( UtilCounters.NUM_EDGE_COUNTER ).increment( 1 );
	}
}
