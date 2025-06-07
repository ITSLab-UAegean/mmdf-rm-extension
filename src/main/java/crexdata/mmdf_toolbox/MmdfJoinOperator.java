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

import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.belt.table.Table;
import com.rapidminer.connection.util.ConnectionInformationSelector;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.LogService;
import ioobject.KStreamDataContainer;

import java.util.List;
import java.util.Objects;
import java.util.logging.Level;

public class MmdfJoinOperator extends MmdfAbstractNodeOperator {
    private final InputPort source1 = getInputPorts().createPort("left source1");
    private final InputPort source2 = getInputPorts().createPort("right source2");
    private OutputPort output = getOutputPorts().createPort("out stream");



    public MmdfJoinOperator(OperatorDescription description) {
        super(description);
    }


    @Override
    public void doWork() throws OperatorException{

        KStreamDataContainer s1 = source1.getData(KStreamDataContainer.class);
        KStreamDataContainer s2 = source2.getData(KStreamDataContainer.class);
        KStreamDataContainer out = output.getData(KStreamDataContainer.class);

        LogService.getRoot().log(Level.INFO,"MMDF testing"+s1.getOUTPUT()+","+s2.getOUTPUT()+"-->"+out.getINPUT());
    }

    @Override
    public List<ParameterType> getParameterTypes(){
        List<ParameterType> types = super.getParameterTypes();
        types.add(new ParameterTypeString(
                "type field",
                "This parameter defines which text is logged to the console when this operator is executed.",
                "join",
                false));
        types.add(new ParameterTypeString(
                "source left",
                "This parameter defines which text is logged to the console when this operator is executed.",
                "",
                false));
        types.add(new ParameterTypeString(
                "source right",
                "This parameter defines which text is logged to the console when this operator is executed.",
                "lat",
                false));
        types.add(new ParameterTypeString(
                "output field",
                "This parameter defines which text is logged to the console when this operator is executed.",
                "",
                false));
        types.add(new ParameterTypeString(
                "spatial index  field",
                "This parameter defines which text is logged to the console when this operator is executed.",
                "h3",
                false));
        types.add(new ParameterTypeInt(
                "dt_ms",
                "This parameter defines which text is logged to the console when this operator is executed.",
                0,
                10000));
        types.add(new ParameterTypeInt(
                "distance",
                "This parameter defines which text is logged to the console when this operator is executed.",
                0,
                1000));
        types.add(new ParameterTypeInt(
                "spatial_resolution",
                "This parameter defines which text is logged to the console when this operator is executed.",
                13,
                8));
        types.add(new ParameterTypeBoolean(
                "topic",
                "This parameter defines which text is logged to the console when this operator is executed.",
                false));

        return  types;
    }


}
