# mmdf-toolbox rm extension

The data fusion toolkit core and rm extension are built upon apache kafka-streams. The methodology focuses on 
spatio-temporal joins for streamming data. 

It requires :
 - All data sources to have some kind metadata / attributes to  extract /use for  spatial and temporal fusion. 
   - e.g If a data source is a video stream , text or anything else the mmdf toolbox will use the metadata (spatial,temporal) to fuse information.
 - The latest message from each topic for each identifier can wait in state for a maximum number of seconds (join_temporal_window_secs) and if spatial conditions (join_spatial_resolution) are met between two records , then the records are fused and sent to the operator output .
 - As soon as a new message arrives it replaces the old one in state and updates its timestamp.
 - As soon as (join_temporal_window_secs) expires for a message, it is discarded from state unless stated explicitly otherwise from design (stateful mechanism).


## Core concepts and workflow:
### Workflow-generation

In RM studio the end-user provides a information required to design the fusion workflow .
E.g a series of streaming operators that register existing kafka topic, transform and join  kafka topics.
The RM extension facilitates the generation of the data fusion specification file in JSON format. In addition, 
it executes the MMDF toolbox core service inside RM studio.

The specification file declares:
- A list of sources (kafka topics)
- A list of tranformations
- A list or pairwise relations.

A single background service will be initiated per fusion operator in the workflow.

#### Runtime-environment

The MMDF is an service that consumes the MMDF JSON specification file and starts the necessary processes
All sources are accessible from the service
Intermediate results are written either on a in-memory state object (or they are published on an intermediate topic)
methodology



## Code Availability
There are two repositories consist the mmdf toolbox.



The RapidMiner installable extension repository that requires a fat-jar lib file from the previous repo : https://github.com/ITSLab-UAegean/mmdf-rm-extension
Defines required custom rapidminer operators to design a workflow, convert it into the configuration file and initiate the execution of the service in AI studio.

Core service code that can be executed as a stand-alone java application: https://github.com/ITSLab-UAegean/mmdf-kstreams
It requires a JSON configuration file as input for execution.


RapidMiner Extension Template
=============================

A template project for creating a RapidMiner Studio extension. 

### Prerequisite
* Requires Gradle 8.2.1+ (use the Gradle wrapper shipped with this template or get it [here](http://gradle.org/installation))

* Requires Java 11

### Getting started
1. Clone the extension template

2. Change the extension settings in _build.gradle_ (e.g. replace 'Template' by the desired extension name)

3. Initialize the extension project by executing the _initializeExtensionProject_ Gradle task (e.g. via 'gradlew initializeExtensionProject')

4. Add an extension icon by placing an image named "icon.png" in  _src/main/resources/META-INF/_. 

5. Build and install your extension by executing the _installExtension_ Gradle task 

6. Start RapidMiner Studio and check whether your extension has been loaded

7. If you want to go back to the empty project with the _initializeExtensionProject_ task, use the _resetExtension_ Gradle task


