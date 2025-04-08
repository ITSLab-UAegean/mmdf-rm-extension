/**
 * RapidMiner Streaming Extension
 *
 * Copyright (C) 2020-2022 RapidMiner GmbH
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