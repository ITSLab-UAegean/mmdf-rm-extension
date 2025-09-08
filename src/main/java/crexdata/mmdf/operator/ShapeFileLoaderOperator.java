/**
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
package crexdata.mmdf.operator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rapidminer.connection.util.ConnectionInformationSelector;
import com.rapidminer.connection.valueprovider.handler.ValueProviderHandlerRegistry;
import com.rapidminer.gui.tools.ProgressThread;
import com.rapidminer.gui.tools.ProgressThreadStoppedException;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.text.Document;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeFile;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.ClassLoaderSwapper;
import com.rapidminer.tools.LogService;
import org.aegean.FusionApp;
import org.aegean.ShapefilePublisherApp;
import org.aegean.fusion.utils.loaders.ShapefileLoader;
import shadow.org.apache.kafka.clients.producer.KafkaProducer;
import shadow.org.apache.kafka.common.serialization.Serdes;
import shadow.org.apache.kafka.common.serialization.StringSerializer;
import shadow.org.apache.kafka.streams.StreamsConfig;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

/**
 * Calculates the sum of two integers.
 *
 * @author gspiliopoulos@aegean.gr
 */
public class ShapeFileLoaderOperator extends Operator {
    private final OutputPort app_id =getOutputPorts().createPort("appId");
    private final ConnectionInformationSelector connectionSelector = new ConnectionInformationSelector(this, "kafka_connector:kafka");



    public ShapeFileLoaderOperator(OperatorDescription description) {
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
        Properties props = new Properties();
        props.put("application.id",app_id );
        props.put("bootstrap.servers", connConfig.get("cluster_config.host_ports"));
        props.put("security.protocol", "SASL_SSL");//connConfig.getValue("cluster_config.auth_method"));
//        props.put("default.key.serde", Serdes.StringSerde.class);
//        props.put("default.value.serde", Serdes.StringSerde.class);


        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("state.dir", "/Users/"+System.getProperty("user.name")+"/.AltairRapidMiner/AI Studio/shared\n");
        // SASL/SSL security configurations
        props.put("sasl.mechanism", "PLAIN");       // SASL mechanism (use PLAIN or other if configured differently)

        if (Objects.equals(props.get("sasl.mechanism").toString(), "SCRAM-SHA-256")){
            props.put("sasl.jaas.config",  shadow.org.apache.kafka.common.security.scram.ScramLoginModule.class.getName() +" required " +
                    "username=\"" +  connConfig.get("cluster_config.username") + "\" password=\"" + connConfig.get("cluster_config.password")+ "\";"); // JAAS login configuration

        }else{
            props.put("sasl.jaas.config", shadow.org.apache.kafka.common.security.plain.PlainLoginModule.class.getName() +" required " +
                    "username=\"" +  connConfig.get("cluster_config.username") + "\" password=\"" + connConfig.get("cluster_config.password")+ "\";"); // JAAS login configuration
        }
        props.put("ssl.truststore.location", connConfig.get("cluster_config.ssl_trust_store_location")); // Path to your truststore
        props.put("ssl.truststore.password", connConfig.get("cluster_config.ssl_trust_store_password"));      // Truststore password
        props.put("ssl.keystore.location", connConfig.get("cluster_config.ssl_key_store_location"));     // Path to your keystore
        props.put("ssl.keystore.password", connConfig.get("cluster_config.ssl_key_store_password"));          // Keystore password

        props.put("ssl.key.password",connConfig.get("cluster_config.ssl_trust_store_password"));
        props.put("commit.interval.ms", 10);
        props.put("linger.ms", 5);
        props.put("batch.size", 1024*2);
        props.put("auto.offset.reset", "latest");



        LogService.getRoot().log(Level.INFO, "MMDF Kafka -- Connection info accepted ");
        LogService.getRoot().log(Level.INFO, props.toString());
        List<String> topics = new ArrayList<>();
        topics.add("test_new");

        LogService.getRoot().log(Level.INFO, "MMDF Kafka -- JAVA CLASS PATH RUNTIME ");
        LogService.getRoot().log(Level.INFO,System.getProperty("java.class.path"));

        File file = this.getParameterAsFile("shapefile_url");
        ProgressThread thread = new ProgressThread("Shapefile-Loader-app:"+app_id) {


                @Override
                public void run() {
                    try (ClassLoaderSwapper cls2 = ClassLoaderSwapper.withContextClassLoader(FusionApp.class.getClassLoader())) {
                        ShapefilePublisherApp.execute(props, file.getPath(), LogService.getRoot(),this::checkCancelled);
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
        types.add(new ParameterTypeFile("shapefile_url","The location of the shapefile",".shp",false));


        return  types;
    }


}
