package crexdata.mmdf.operator.config;

import shadow.org.apache.kafka.common.serialization.Serdes;
import shadow.org.apache.kafka.streams.StreamsConfig;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public  class KafkaPropertiesProvider {

    public static Properties get_basic_kafka_configuration(Map<String, String> connConfig,String app_id, String state_dir) {
        Properties props = new Properties();
        props.put("application.id", app_id);
        props.put("bootstrap.servers", connConfig.get("cluster_config.host_ports"));
        props.put("security.protocol", "SASL_SSL");//connConfig.getValue("cluster_config.auth_method"));
//        props.put("default.key.serde", Serdes.StringSerde.class);
//        props.put("default.value.serde", Serdes.StringSerde.class);


        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

//        if (this.getParameterAsFile("state.dir") == null || !this.getParameterAsFile("state.dir").exists()) {
////            props.put("state.dir", "/Users/" + System.getProperty("user.name") + "/.AltairRapidMiner/AI Studio/shared\n");
//            props.put("state.dir", System.getProperty("user.home")+"/.AltairRapidMiner/AI Studio/shared\n");
//        }else{
//            props.put("state.dir", this.getParameterAsFile("state.dir").getPath());
//        }

        props.put("state.dir",state_dir);
        // SASL/SSL security configurations
        // SASL mechanism (use PLAIN or other if configured differently)

        if (!connConfig.get("cluster_config.auth_method").equals("none")) {
            props.put("sasl.mechanism", "PLAIN");


            if (Objects.equals(props.get("sasl.mechanism").toString(), "SCRAM-SHA-256")) {

                props.put("sasl.jaas.config", shadow.org.apache.kafka.common.security.scram.ScramLoginModule.class.getName() + " required " +
                        "username=\"" + connConfig.get("cluster_config.username") + "\" password=\"" + connConfig.get("cluster_config.password") + "\";"); // JAAS login configuration

            } else {

                if (connConfig.containsKey("cluster_config.username")) {
                    props.put("sasl.jaas.config", shadow.org.apache.kafka.common.security.plain.PlainLoginModule.class.getName() + " required " +
                            "username=\"" + connConfig.get("cluster_config.username") + "\" password=\"" + connConfig.get("cluster_config.password") + "\";"); // JAAS login configuration
                } else {

                    props.put("sasl.jaas.config", shadow.org.apache.kafka.common.security.plain.PlainLoginModule.class.getName() + ";"); //
                }
            }


            if (connConfig.containsKey("cluster_config.ssl_trust_store_location")) {
                props.put("ssl.truststore.location", connConfig.get("cluster_config.ssl_trust_store_location")); // Path to your truststore

            }

            if (connConfig.containsKey("cluster_config.ssl_trust_store_password")) {
                props.put("ssl.truststore.password", connConfig.get("cluster_config.ssl_trust_store_password"));      // Truststore password
                props.put("ssl.key.password", connConfig.get("cluster_config.ssl_trust_store_password"));
            }
            if (connConfig.containsKey("cluster_config.ssl_key_store_location")) {
                props.put("ssl.keystore.location", connConfig.get("cluster_config.ssl_key_store_location"));     // Path to your keystore
            }
            if (connConfig.containsKey("cluster_config.ssl_key_store_password")) {
                props.put("ssl.keystore.password", connConfig.get("cluster_config.ssl_key_store_password"));          // Keystore password
            }
        } else {

            props.put("security.protocol", "PLAINTEXT");
        }
        props.put("commit.interval.ms", 10);
        props.put("linger.ms", 5);
        props.put("batch.size", 1024 * 2);
        props.put("auto.offset.reset", "latest");
        return(props);
    }

}
