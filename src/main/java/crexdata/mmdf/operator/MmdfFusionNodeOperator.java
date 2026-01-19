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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rapidminer.connection.util.ConnectionInformationSelector;
import com.rapidminer.connection.valueprovider.handler.ValueProviderHandlerRegistry;
import com.rapidminer.gui.tools.ProgressThread;
import com.rapidminer.gui.tools.ProgressThreadStoppedException;
import com.rapidminer.operator.*;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.text.Document;
import com.rapidminer.parameter.*;
import com.rapidminer.tools.ClassLoaderSwapper;
import com.rapidminer.tools.LogService;
import crexdata.mmdf.operator.config.KafkaPropertiesProvider;
import org.aegean.FusionApp;
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
public class MmdfFusionNodeOperator extends Operator {
    private final InputPort fusion_conf = getInputPorts().createPort("fusion_conf");
    private final OutputPort app_id =getOutputPorts().createPort("appId");
    private final ConnectionInformationSelector connectionSelector = new ConnectionInformationSelector(this, "kafka_connector:kafka");


    private Stack<MmdfFusionNodeOperator> operators = new Stack<>();
    public MmdfFusionNodeOperator(OperatorDescription description) {
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
        LogService.getRoot().log(Level.INFO, "MMDF testing");
        Map<String, String> connConfig = ValueProviderHandlerRegistry
                .getInstance()
                .injectValues(connectionSelector.getConnection(), this, false);

        Document doc = fusion_conf.getData(com.rapidminer.operator.text.Document.class);
        LogService.getRoot().log(Level.INFO, doc.toString());

        try {
            Class.forName(shadow.org.apache.kafka.common.security.plain.PlainLoginModule.class.getName());
            Class.forName(shadow.org.apache.kafka.common.serialization.Serdes.class.getName());
            Class.forName(shadow.org.apache.kafka.clients.admin.AdminClient.class.getName());

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        String app_id = "fusion-pipe-" + UUID.randomUUID();
        Document appDoc = new Document(app_id);
//        appDoc.setUserData("app_id",app_id);
        this.app_id.deliver(appDoc);



        Properties props = KafkaPropertiesProvider.get_basic_kafka_configuration(connConfig, app_id, "/tmp/");
        if (this.getParameterAsFile("state.dir") == null || !this.getParameterAsFile("state.dir").exists()) {
            props.put("state.dir", System.getProperty("user.home") + "/.AltairRapidMiner/AI Studio/shared\n");
        } else {
            props.put("state.dir", this.getParameterAsFile("state.dir").getPath());
        }
        props.put("auto.offset.reset", this.getParameterAsString("auto.offset.reset"));



        LogService.getRoot().log(Level.INFO, "MMDF Kafka -- Connection info accepted OK ");
        LogService.getRoot().log(Level.ALL, props.toString());


        LogService.getRoot().log(Level.INFO, "MMDF Kafka -- JAVA CLASS PATH RUNTIME  OK");
        LogService.getRoot().log(Level.ALL, System.getProperty("java.class.path"));

        if (this.getParameterAsBoolean("run.in.thread")) {

            ProgressThread thread = new ProgressThread("mmdf-kstreams:" + app_id) {
                @Override
                public void run() {
                    try {
                        String mmdf_config = doc.getTokenText();
                        run_execution(props, mmdf_config, this::checkCancelled);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to execute MMDF" + e.getMessage());
                    }
                }


            };
            thread.start();


        } else {
            String mmdf_config = doc.getTokenText();
            run_execution(props, mmdf_config, () -> {
                try {
                    checkForStop();
                } catch (ProcessStoppedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    /****
     *  Gets parameters
     ***/
    @Override
    public List<ParameterType> getParameterTypes(){
        List<ParameterType> types = super.getParameterTypes();
        types.add(new ParameterTypeCategory(
                "auto.offset.reset",
                "kafka reading from latest or earliest values",
                new String[]{"latest","earliest",},
                0
                )
        );
        types.add(new ParameterTypeBoolean(
                        "run.in.thread",
                        "sends execution to background thread (AI Studio)",
                        true,
                        true
                )
        );



        types.add(new ParameterTypeDirectory("state.dir","State Directory Location","/tmp/"));

//        types.add(new ParameterTypeString("state.dir","State Directory Location",true));


        return  types;
    }

    public void run_execution(Properties props,String mmdf_config,Runnable stop_check) throws WrapperOperatorRuntimeException {
        try (ClassLoaderSwapper cls2 = ClassLoaderSwapper.withContextClassLoader(FusionApp.class.getClassLoader())) {

            ObjectMapper mapper = new ObjectMapper();
            ObjectNode base = (ObjectNode) mapper.readTree(mmdf_config);

            if (base.has("payload")) {
                ObjectNode payload = (ObjectNode) base.get("payload");

                if (!payload.has("transformations")) {
                    payload.putArray("transformations");
                }
                if (!payload.has("relations")) {
                    payload.putArray("relations");
                }

                String config = mapper.writeValueAsString(payload);

                LogService.getRoot().log(Level.INFO, config);
                FusionApp.execute(props, config, LogService.getRoot(), stop_check);
            } else {

                if (!base.has("transformations")) {
                    base.putArray("transformations");
                }
                if (!base.has("relations")) {
                    base.putArray("relations");
                }
                FusionApp.execute(props, mmdf_config, LogService.getRoot(),stop_check);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute MMDF" + e.getMessage());
        }
    }

}
