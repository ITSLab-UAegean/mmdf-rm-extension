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
package crexdata.mmdf_toolbox;

import com.rapidminer.connection.configuration.ConnectionConfiguration;
import com.rapidminer.connection.util.ConnectionInformationSelector;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.text.Document;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.ClassLoaderSwapper;
import com.rapidminer.tools.LogService;

import org.aegean.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;


import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import static com.rapidminer.extension.kafka_connector.PluginInitKafkaConnector.getPluginLoader;

/**
 * Calculates the sum of two integers.
 *
 * @author gspiliopoulos@aegean.gr
 */
public class MmdfFusionNodeOperator extends Operator {
    private final InputPort fusion_conf = getInputPorts().createPort("fusion_conf");
    private final ConnectionInformationSelector connectionSelector = new ConnectionInformationSelector(this, "kafka_connector:kafka");


    private Stack<MmdfFusionNodeOperator> operators = new Stack<>();
    public MmdfFusionNodeOperator(OperatorDescription description) {
        super(description);
    }

    @Override
    public void doWork() throws OperatorException {
        ConnectionConfiguration connConfig = connectionSelector.getConnection().getConfiguration();

//        Properties clusterConfig = KafkaConnectionHandler.getINSTANCE().buildClusterConfiguration(connConfig);
        try {
            doDefaultWork();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

//        FusionApp.main();

    }

    /***
     * Does default work
     * @throws OperatorException if the operation fails
     ***/
    public void doDefaultWork() throws OperatorException, IOException {
        LogService.getRoot().log(Level.INFO, "MMDF testing");
        ConnectionConfiguration connConfig = connectionSelector.getConnection().getConfiguration();

        Document doc = fusion_conf.getData(com.rapidminer.operator.text.Document.class);
        LogService.getRoot().log(Level.INFO, doc.toString());
        //connConfig.getKeyMap().forEach((x,p)-> LogService.getRoot().log(Level.INFO, x+"->"+p.getValue()));


        try {
            Class.forName(org.apache.kafka.common.security.plain.PlainLoginModule.class.getName() );
            Class.forName(org.apache.kafka.common.serialization.Serdes.class.getName());
            Class.forName(org.apache.kafka.clients.admin.AdminClient.class.getName());

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        Properties props = new Properties();

        props.put("application.id", "fusion-pipe-" + UUID.randomUUID());
        props.put("bootstrap.servers", connConfig.getValue("cluster_config.host_ports"));
        props.put("security.protocol", "SASL_SSL");//connConfig.getValue("cluster_config.auth_method"));
//        props.put("default.key.serde", Serdes.StringSerde.class);
//        props.put("default.value.serde", Serdes.StringSerde.class);


        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
//        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
//        props.remove("default.key.serde");
//        props.remove("default.value.serde");

//        props.put(StreamsConfig.STATE_DIR_CONFIG, "/your/extension/writable/dir");
        props.put("state.dir", "/Users/giannis/.AltairRapidMiner/AI Studio/shared\n");
        // SASL/SSL security configurations
        props.put("sasl.mechanism", "PLAIN");       // SASL mechanism (use PLAIN or other if configured differently)
        //props.put("sasl.mechanism",connConfig.getValue("cluster_config.scram_level"));



        if (Objects.equals(props.get("sasl.mechanism").toString(), "SCRAM-SHA-256")){
            props.put("sasl.jaas.config",  org.apache.kafka.common.security.scram.ScramLoginModule.class.getName() +" required " +
                    "username=\"" +  connConfig.getValue("cluster_config.username") + "\" password=\"" + connConfig.getValue("cluster_config.password")+ "\";"); // JAAS login configuration

        }else{
            props.put("sasl.jaas.config", org.apache.kafka.common.security.plain.PlainLoginModule.class.getName() +" required " +
                    "username=\"" +  connConfig.getValue("cluster_config.username") + "\" password=\"" + connConfig.getValue("cluster_config.password")+ "\";"); // JAAS login configuration

        }
        props.put("ssl.truststore.location", connConfig.getValue("cluster_config.ssl_trust_store_location")); // Path to your truststore
        props.put("ssl.truststore.password", connConfig.getValue("cluster_config.ssl_trust_store_password"));      // Truststore password
        props.put("ssl.keystore.location", connConfig.getValue("cluster_config.ssl_key_store_location"));     // Path to your keystore
        props.put("ssl.keystore.password", connConfig.getValue("cluster_config.ssl_key_store_password"));          // Keystore password

        props.put("ssl.key.password",connConfig.getValue("cluster_config.ssl_trust_store_password"));
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

//        Class.forName("org.apache.kafka.common.security.plain.PlainLoginModule");

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            createTopicsIfMissing(topics,props);
            return null;
        });

        try (ClassLoaderSwapper cls = ClassLoaderSwapper.withContextClassLoader(getPluginLoader()); ClassLoaderSwapper cls2 = ClassLoaderSwapper.withContextClassLoader(FusionApp.class.getClassLoader())){
//            try (ClassLoaderSwapper cls = ClassLoaderSwapper.withContextClassLoader(getPluginLoader())){

//            LogService.getRoot().log(Level.INFO,cls2.toString());
////            cls.wait(1000);
////            cls2.wait(1000);
//            LogService.getRoot().log(Level.INFO,cls2.toString());


//            String config = "{\"sources\":[{\"name\":\"ais\",\"id\":\"mmsi\",\"lon\":\"lon\",\"lat\":\"lat\",\"t\":\"timestamp\",\"expires_ms\":2147483647},{\"name\":\"ccc-position\",\"id\":\"mmsi\",\"lon\":\"lon\",\"lat\":\"lat\",\"t\":\"timestamp\",\"expires_ms\":2147483647},{\"name\":\"crex-boundaries\",\"id\":\"h3\",\"lon\":\"lon\",\"lat\":\"lat\",\"t\":\"timestamp\",\"expires_ms\":2147483647,\"type\":\"shapefile\"},{\"name\":\"crex-video\",\"id\":\"h3\",\"lon\":\"lon\",\"lat\":\"lat\",\"t\":\"timestamp\",\"expires_ms\":2147483647,\"resolution\":9,\"type\":\"video\"}],\"transformations\":[{\"source\":\"ccc-position\",\"method\":\"flatMap\",\"field\":\"h3\",\"value\":1,\"topic\":false,\"output\":\"ccc-position\"},{\"source\":\"crex-maritime-fused\",\"method\":\"filter\",\"field\":\"sog\",\"value\":0.0,\"topic\":true,\"operator\":\"gt\",\"output\":\"crexdata-maritime-fused-sog\"},{\"source\":\"crexdata-maritime-fused-sog\",\"method\":\"kplerCA\",\"field\":\"h3\",\"value\":0.0,\"topic\":true,\"output\":\"crex-maritime-ca\"}],\"relations\":[{\"type\":\"merge\",\"source_left\":\"ais\",\"source_right\":\"ccc-position\",\"output_name\":\"merged-ais-ccc-position\",\"spatial_index\":\"h3\",\"dt_ms\":10000,\"distance\":10,\"spatial_resolution\":9,\"topic\":false},{\"type\":\"join\",\"source_left\":\"merged-ais-ccc-position\",\"source_right\":\"ccc-position\",\"output_name\":\"crex-maritime-fused\",\"topic\":true,\"spatial_index\":\"h3\",\"dt_ms\":2000,\"distance\":1000,\"spatial_resolution\":9},{\"type\":\"leftJoinAsTable\",\"source_left\":\"crexdata-maritime-fused-sog\",\"source_right\":\"crex-boundaries\",\"output_name\":\"ccc-fused-shapefile\",\"spatial_index\":\"h3\",\"dt_ms\":10000,\"distance\":1000,\"spatial_resolution\":9,\"topic\":false,\"fields\":\"wkt,timestamp\"},{\"type\":\"leftJoinAsTable\",\"source_left\":\"ccc-fused-shapefile\",\"source_right\":\"crex-video\",\"output_name\":\"ccc-fused-final\",\"spatial_index\":\"h3\",\"dt_ms\":10000,\"distance\":1000,\"spatial_resolution\":9,\"topic\":true,\"fields\":\"url,timestamp\"}]}";

            Executors.newSingleThreadExecutor().submit(() -> {


                FusionApp.execute(props, doc.getTokenText(),LogService.getRoot());
            });




        }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to execute MMDF", e);
        }

    }

    /****
     *  Gets parameters
     ***/
    @Override
    public List<ParameterType> getParameterTypes(){
        List<ParameterType> types = super.getParameterTypes();
        types.add(new ParameterTypeString(
                "configuration",
                "This should be a json configuration file",
                "",
                false));

        return  types;
    }


    public static void createTopicsIfMissing(Collection<String> topicNames, Properties kafkaProps) {


        try (ClassLoaderSwapper cls = ClassLoaderSwapper.withContextClassLoader(getPluginLoader());AdminClient adminClient = AdminClient.create(kafkaProps)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            List<NewTopic> topicsToCreate = new ArrayList<>();

            for (String topic : topicNames) {
                if (!existingTopics.contains(topic)) {
                    topicsToCreate.add(new NewTopic(topic, 1, (short) 1)); // Adjust partitions and replication as needed
                }
            }

            if (!topicsToCreate.isEmpty()) {
                adminClient.createTopics(topicsToCreate).all().get();
                System.out.println("Created missing topics: " + topicsToCreate);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create topics", e);
        }
    }
}
