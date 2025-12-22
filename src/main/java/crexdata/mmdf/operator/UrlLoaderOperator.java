package crexdata.mmdf.operator; /**
 * Crexdata Project
 *
 * Copyright (C) 2025-2025 by Crexdata Project and the contributors
 *
 * Complete list of developers available at our web site:
 *
 *      https://altair.com
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

import com.rapidminer.connection.util.ConnectionInformationSelector;
import com.rapidminer.connection.valueprovider.handler.ValueProviderHandlerRegistry;
import com.rapidminer.gui.tools.ProgressThread;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.text.Document;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDirectory;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.ClassLoaderSwapper;
import com.rapidminer.tools.LogService;
import crexdata.mmdf.operator.config.KafkaPropertiesProvider;
import crexdata.mmdf.operator.utils.ParameterDescriptionEnum;
import org.aegean.FusionApp;
import org.aegean.VideoStreamPublisherApp;
import shadow.org.apache.kafka.common.serialization.Serdes;
import shadow.org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

/**
 * Calculates the sum of two integers.
 *
 * @author gspiliopoulos@aegean.gr
 */
public class UrlLoaderOperator extends Operator {
    private final OutputPort app_id =getOutputPorts().createPort("appId");
    private final ConnectionInformationSelector connectionSelector = new ConnectionInformationSelector(this, "kafka_connector:kafka");



    public UrlLoaderOperator(OperatorDescription description) {
        super(description);
    }

    @Override
    public void doWork() throws OperatorException {

        try {
            doDefaultWork();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /***
     * Does default work
     * @throws OperatorException if the operation fails
     ***/
    public void doDefaultWork() throws OperatorException, IOException {

        LogService.getRoot().log(Level.INFO, "MMDF Shapefile loading");
        Map<String, String> connConfig = ValueProviderHandlerRegistry
                .getInstance()
                .injectValues(connectionSelector.getConnection(), this, false);


        try {
            Class.forName(shadow.org.apache.kafka.common.security.plain.PlainLoginModule.class.getName() );
            Class.forName(Serdes.class.getName());
            Class.forName(shadow.org.apache.kafka.clients.admin.AdminClient.class.getName());

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        String app_id= "fusion-pipe-" + UUID.randomUUID();
        Document appDoc = new Document(app_id);
//        appDoc.setUserData("app_id",app_id);
        this.app_id.deliver(appDoc);

        Properties props = KafkaPropertiesProvider.get_basic_kafka_configuration(connConfig,app_id,"/tmp/");
        if (this.getParameterAsFile("state.dir") == null || !this.getParameterAsFile("state.dir").exists()) {
//            props.put("state.dir", "/Users/" + System.getProperty("user.name") + "/.AltairRapidMiner/AI Studio/shared\n");
            props.put("state.dir", System.getProperty("user.home")+"/.AltairRapidMiner/AI Studio/shared\n");
        }else{
            props.put("state.dir", this.getParameterAsFile("state.dir").getPath());
        }

        LogService.getRoot().log(Level.INFO, "MMDF Kafka -- Connection info accepted ");
        LogService.getRoot().log(Level.INFO, props.toString());
        List<String> topics = new ArrayList<>();
        topics.add("test_new");

        LogService.getRoot().log(Level.INFO, "MMDF Kafka -- JAVA CLASS PATH RUNTIME ");
        LogService.getRoot().log(Level.INFO,System.getProperty("java.class.path"));


        String url  = this.getParameterAsString("url");
        String wkt  = this.getParameterAsString("wkt");
        String topic  = this.getParameterAsString("topic");
        Integer resolution  = this.getParameterAsInt("resolution");
        ProgressThread thread = new ProgressThread("Shapefile-Loader-app:"+app_id) {


                @Override
                public void run() {
                    try (ClassLoaderSwapper cls2 = ClassLoaderSwapper.withContextClassLoader(FusionApp.class.getClassLoader())) {
                        VideoStreamPublisherApp.execute(props, topic, url, wkt, resolution, LogService.getRoot(),this::checkCancelled);
                    } catch (Exception e) {
                        LogService.getRoot().log(Level.INFO, "MMDF Kafka -- EXECUTION STOPPED");
                    }
                }
        };
        thread.start();

    }

    /****
     *  Gets parameters
     ***/
    @Override
    public List<ParameterType> getParameterTypes(){
        List<ParameterType> types = super.getParameterTypes();
        types.add(new ParameterTypeString("url","The url to be shared","",false));
        types.add(new ParameterTypeString("wkt","The footprint/related area of the url in well known text format","",false));
        types.add(new ParameterTypeString("topic","The name of the topic","crex-video",false));
        types.add(new ParameterTypeInt(
                "resolution",
                ParameterDescriptionEnum.H3_RESOLUTION.getLabel(),
                2,
                13));
        types.add(new ParameterTypeDirectory("state.dir","State Directory Location","/tmp/"));

        return  types;
    }


}
