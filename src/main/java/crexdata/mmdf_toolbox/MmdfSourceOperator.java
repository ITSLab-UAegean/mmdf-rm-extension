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
package crexdata.mmdf_toolbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.text.Document;
import com.rapidminer.parameter.*;
import com.rapidminer.tools.LogService;
import crexdata.mmdf_toolbox.utils.ParameterDescriptionEnum;

import java.util.List;
import java.util.logging.Level;


public class MmdfSourceOperator extends MmdfAbstractNodeOperator {
//    protected final ConnectionInformationSelector connectionSelector = new ConnectionInformationSelector(this, "kafka_connector:kafka");
////    private InputPort kafka_connection = getInputPorts().createPort("in stream");
//    private OutputPort outputPort = getOutputPorts().createPort("out config");
    private OutputPort documentPort = getOutputPorts().createPort("document");


    public MmdfSourceOperator(OperatorDescription description) {
        super(description);
        getTransformer().addGenerationRule(documentPort, Document.class);
    }

    @Override
    public void doWork() throws OperatorException{


        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ObjectNode source = mapper.createObjectNode();

        ArrayNode sources = root.has("sources")? (ArrayNode) root.get("sources") : root.putArray("sources");

        this.getParameterTypes().forEach(p->{
            try {
                source.put(p.getKey(),this.getParameter(p.getKey()));
            } catch (UndefinedParameterError e) {
                throw new RuntimeException(e);
            }
        });
        sources.add(source);
        try {
            ObjectNode base = mapper.createObjectNode();
            base.put("output",this.getParameter("name"));
            base.put("payload",root);

            String config = mapper.writeValueAsString(base);
            documentPort.deliver( new Document(config));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        LogService.getRoot().log(Level.INFO,"MMDF testing");
    }

    @Override
    public List<ParameterType> getParameterTypes(){
        List<ParameterType> types = super.getParameterTypes();
        types.add(new ParameterTypeString(
                "name",
                ParameterDescriptionEnum.INPUT_TOPIC_NAME.getLabel(),
                "",
                false));
        types.add(new ParameterTypeString(
                "lon",
                ParameterDescriptionEnum.LONGITUDE.getLabel(),
                "lon",
                false));
        types.add(new ParameterTypeString(
                "lat",
                ParameterDescriptionEnum.LATITUDE.getLabel(),
                "lat",
                false));
        types.add(new ParameterTypeString(
                "id",
                ParameterDescriptionEnum.ID.getLabel(),
                "id",
                false));
        types.add(new ParameterTypeString(
                "t",
                ParameterDescriptionEnum.TIME.getLabel(),
                "timestamp",
                false));
        types.add(new ParameterTypeInt(
                "resolution",
                ParameterDescriptionEnum.H3_RESOLUTION.getLabel(),
                8,
                13));
        types.add(new ParameterTypeInt(
                "expires_ms",
                ParameterDescriptionEnum.EXPIRE_MS.getLabel(),                0,
                600000));
        types.add(new ParameterTypeBoolean(
                "peek",
                ParameterDescriptionEnum.PEEK.getLabel(),
                false,
                false));

        return  types;
    }


}
