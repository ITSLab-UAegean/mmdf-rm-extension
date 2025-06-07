/**
 * Crexdata Project
 *
 * Copyright (C) 2025-2025 by Crexdata Project and the contributors
 *
 * Complete list of developers available at our web site:
 *
 *      https://crexdata.eu/
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/.
 */
package ioobject;


import com.rapidminer.operator.ResultObjectAdapter;


/**
 * IO object/container for data that needs to traverse through the operator graph when building RapidMiner processes
 *
 * @author Mate Torok
 * @since 0.1.0
 */
public class KStreamDataContainer extends ResultObjectAdapter {

	private static final long serialVersionUID = 1L;
	private final String name;
	private final String INPUT;
	private final String OUTPUT;

//	private final StreamGraph streamGraph;
//
//	private final StreamProducer lastNode;

	public KStreamDataContainer(String name,  String input, String output) {
//		this.streamGraph = streamGraph;
//		this.lastNode = lastNode;
        this.name = name;
        INPUT = input;
        OUTPUT = output;
    }

	public String getOUTPUT(){
		return  this.OUTPUT;
	}

	public String getINPUT(){
		return  this.INPUT;
	}
	/**
	 * @return platform independent stream graph
	 */


	@Override
	public String toString() {
		return "StreamDataContainer{" +
			"name=" + this.name +
			"IN=" + this.INPUT +
			"OUTPUT=" + this.OUTPUT +
			'}';
	}

}