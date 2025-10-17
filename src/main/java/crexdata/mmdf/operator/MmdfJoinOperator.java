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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

public class MmdfJoinOperator extends MmdfAbstractNodeOperator {
    private final InputPort source1 = getInputPorts().createPort("left");
    private final InputPort source2 = getInputPorts().createPort("right");


    private OutputPort documentPort = getOutputPorts().createPort("out stream");



    public MmdfJoinOperator(OperatorDescription description)  {
        super(description);
        getTransformer().addGenerationRule(documentPort, Document.class);
    }


    @Override
    public void doWork() throws OperatorException{
        Document s1 = source1.getData(Document.class);
        Document s2 = source2.getData(Document.class);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode base = mapper.createObjectNode();

        ArrayList<ObjectNode> docs = new ArrayList<>();
        ObjectNode os1=null;
        ObjectNode os2=null;
        try {
            os1 = (ObjectNode) mapper.readTree(s1.getTokenText());
            docs.add(os1);
            os2 = (ObjectNode) mapper.readTree(s2.getTokenText());
            docs.add(os2);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }


        ArrayNode sources = base.putArray("sources");
        ArrayNode transformations = base.putArray("transformations");
        ArrayNode relations = base.putArray("relations");

        try {

            ArrayList<String> source_outputs = new ArrayList<>();
            ArrayList<String> tranformation_outputs = new ArrayList<>();
            ArrayList<String> relation_outputs = new ArrayList<>();


            docs.forEach(doc->{

                ObjectNode root = (ObjectNode) doc.get("payload");
                ArrayNode doc_sources = root.has("sources")? (ArrayNode) root.get("sources") : root.putArray("sources");
                ArrayNode doc_transformations = root.has("transformations")? (ArrayNode) root.get("transformations") : root.putArray("transformations");
                ArrayNode doc_relations = root.has("relations")? (ArrayNode) root.get("relations") : root.putArray("relations");

                doc_sources.elements().forEachRemaining(s->{
                    if (source_outputs.isEmpty() || !source_outputs.contains(s.get("name").asText())){
                        sources.add(s);
                        source_outputs.add(s.get("name").asText());
                    }
                });
                doc_transformations.elements().forEachRemaining(s->{
                    if (tranformation_outputs.isEmpty() || !tranformation_outputs.contains(s.get("output").asText())){
                        transformations.add(s);
                        tranformation_outputs.add(s.get("output").asText());
                    }
                });
                doc_relations.elements().forEachRemaining(s->{
                    if (relation_outputs.isEmpty() || !relation_outputs.contains(s.get("output").asText())){
                        relations.add(s);
                        relation_outputs.add(s.get("output").asText());
                    }
                });



            });

            ObjectNode relation = mapper.createObjectNode();
            relation.put("source_left",os1.get("output"));
            relation.put("source_right",os2.get("output"));
            this.getParameterTypes().forEach(p->{
                try {
                    relation.put(p.getKey(),getParameterAsString(p.getKey()));
                } catch (UndefinedParameterError e) {
                    throw new RuntimeException(e);
                }
            });
            relations.add(relation);


            ObjectNode top = mapper.createObjectNode();
            try {
                top.put("output",getParameterAsString("output"));
                top.put("payload", base);
            } catch (UndefinedParameterError e) {
                throw new RuntimeException(e);
            }
            String config = mapper.writeValueAsString(top);
//            outputPort.deliver(new ConfigObjectIOObject(config));

            documentPort.deliver( new Document(config));

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        LogService.getRoot().log(Level.INFO,"MMDF testing"+s1.getTokenText()+","+s2.getTokenText()+"-->"+getParameterAsString("output"));
    }


    @Override
    public List<ParameterType> getParameterTypes(){

        List<ParameterType> types = super.getParameterTypes();
        types.add(new ParameterTypeCategory(
                "type",
                "Type of the join",
                new String[]{"merge","simpleJoin","simpleLeftJoin","join","joinLatest", "joinAsTable","leftJoin","leftJoinAsTable","pass"},
                0));

        types.add(new ParameterTypeString(
                "fields",
                "A comma separated value of fields that will be copied to the result, if  empty all fields are copied",
                true));
        types.add(new ParameterTypeCategory(
                "spatial_index",
                ParameterDescriptionEnum.H3_RESOLUTION.getLabel(),
                new String[]{"h3"},
                0));
        types.add(new ParameterTypeInt(
                "dt_ms",
                ParameterDescriptionEnum.DT_MS.getLabel(),
                0,
                72000000));
        types.add(new ParameterTypeInt(
                "distance",
                ParameterDescriptionEnum.DISTANCE_METERS.getLabel(),
                1,
                1000000));
        types.add(new ParameterTypeInt(
                "spatial_resolution",
                ParameterDescriptionEnum.H3_RESOLUTION.getLabel(),
                2,
                13));
        types.add(new ParameterTypeCategory(
                "id_join_mechanism",
                "Join mechanism with respect to the identifier. ",
                new String[]{"ignore","disjoin","match"},
                0));
        types.add(new ParameterTypeString(
                "records_name",
                ParameterDescriptionEnum.RECORDS_NAME.getLabel(),
                "detections",
                false));
        types.add(new ParameterTypeString(
                "output",
                ParameterDescriptionEnum.OUTPUT_NAME.getLabel(),
                "Name of the  k-stream output topic",
                false));
        types.add(new ParameterTypeBoolean(
                "topic",
                ParameterDescriptionEnum.TOPIC.getLabel(),
                false));
        types.add(new ParameterTypeBoolean(
                "peek",
                ParameterDescriptionEnum.PEEK.getLabel(),
                false,
                false));

        return  types;
    }


}
