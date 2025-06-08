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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

        props.put("state.dir", "/Users/giannis/.AltairRapidMiner/AI Studio/shared\n");
        // SASL/SSL security configurations
        props.put("sasl.mechanism", "PLAIN");       // SASL mechanism (use PLAIN or other if configured differently)

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

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            createTopicsIfMissing(topics,props);
            return null;
        });

        try (ClassLoaderSwapper cls = ClassLoaderSwapper.withContextClassLoader(getPluginLoader()); ClassLoaderSwapper cls2 = ClassLoaderSwapper.withContextClassLoader(FusionApp.class.getClassLoader())){

            Executors.newSingleThreadExecutor().submit(() -> {
                ObjectMapper mapper = new ObjectMapper();
                try {
                    String mmdf_config = doc.getTokenText();
                    ObjectNode base = (ObjectNode) mapper.readTree(mmdf_config);

                    if (base.has("payload")){
                        FusionApp.execute(props, base.get("payload").asText(),LogService.getRoot());
                    }else{
                        FusionApp.execute(props, mmdf_config,LogService.getRoot());
                    }


                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }


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
