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
package crexdata.mmdf.operator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.text.Document;
import com.rapidminer.parameter.*;
import com.rapidminer.tools.LogService;
import crexdata.mmdf.operator.utils.ParameterDescriptionEnum;

import java.util.List;
import java.util.logging.Level;

public class MmdfTransformerOperator extends MmdfAbstractNodeOperator{
    private final InputPort inputPort = getInputPorts().createPort("in config");
    private final OutputPort documentPort = getOutputPorts().createPort("document");

    public MmdfTransformerOperator(OperatorDescription description) {
        super(description);
        getTransformer().addGenerationRule(documentPort, Document.class);

    }

    @Override
    public void doWork() throws OperatorException {
        Document input_io = inputPort.getData(Document.class);

        ObjectMapper mapper = new ObjectMapper();
        try {
            ObjectNode base =  (ObjectNode) mapper.readTree(input_io.getTokenText());
            ObjectNode root = (ObjectNode) base.get("payload");
            ArrayNode transformations = root.has("transformations")? (ArrayNode) root.get("transformations") : root.putArray("transformations");
            ObjectNode transformation = mapper.createObjectNode();
            transformation.put("source",base.get("output"));

            this.getParameterTypes().forEach(p->{
                try {
                    transformation.put(p.getKey(),getParameterAsString(p.getKey()));
                } catch (UndefinedParameterError e) {
                    throw new RuntimeException(e);
                }
            });

            transformations.add(transformation);

            base.put("output",getParameterAsString("output"));
            base.put("payload", root);


            String config = mapper.writeValueAsString(base);
//            outputPort.deliver(new ConfigObjectIOObject(config));


            documentPort.deliver( new Document(config));

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        LogService.getRoot().log(Level.INFO,"MMDF testing");
    }
    
    @Override
    public List<ParameterType> getParameterTypes(){
        List<ParameterType> types = super.getParameterTypes();
        types.add(new ParameterTypeCategory(
                "method",
                "This parameter selects transformation method.",
                new String[]{"filter","flatMap", "kplerCA", "sink", "deduplicate", "asvCommand","interval","extract","udf","expand_json","accumulate"},
                0));
        types.add(new ParameterTypeString(
                "field",
                "The field upon the transformation will be applied. 'filter' requires an operator and a value (empty string if not applicable). 'extract' will extract subtree node. udf will read a math expression from value and store to field. 'expand_json': flattens json formats  ",
                "",
                false));
        types.add(new ParameterTypeCategory(
                "operator",
                "Applies when filter is selected",
                new String[]{"hasvalue","eq","neq","gt","gte","lt","lte","notnull"},
                0));
        types.add(new ParameterTypeString(
                "value",
                "Value the operator is applied on.",
                false));
        types.add(new ParameterTypeString(
                "output",
                ParameterDescriptionEnum.OUTPUT_NAME.getLabel(),
                this.getName(),
                false));

        types.add(new ParameterTypeBoolean(
                "topic",
                ParameterDescriptionEnum.TOPIC.getLabel(),
                false,
                false));

        types.add(new ParameterTypeBoolean(
                "peek",
                ParameterDescriptionEnum.PEEK.getLabel(),
                false,
                false));

        return  types;
    }


}
