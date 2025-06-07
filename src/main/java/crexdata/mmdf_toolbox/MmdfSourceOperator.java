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
import java.util.List;
import java.util.logging.Level;

import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.belt.table.Table;
import com.rapidminer.connection.ConnectionInformation;
import com.rapidminer.connection.util.ConnectionInformationSelector;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.LogService;

public class MmdfSourceOperator extends MmdfAbstractNodeOperator {
    protected final ConnectionInformationSelector connectionSelector = new ConnectionInformationSelector(this, "kafka_connector:kafka");
    private InputPort kafka_connection = getInputPorts().createPort("in stream");
    private OutputPort tableOutput = getOutputPorts().createPort("out stream");


    public MmdfSourceOperator(OperatorDescription description) {
        super(description);
    }

    @Override
    public void doWork() throws OperatorException{
        ConnectionInformationSelector selector= kafka_connection.getData();
        ConnectionInformation connection = selector.getConnection();
        LogService.getRoot().log(Level.INFO,"MMDF testing");
    }

    @Override
    public List<ParameterType> getParameterTypes(){
        List<ParameterType> types = super.getParameterTypes();
        types.add(new ParameterTypeString(
                "name field",
                "This parameter defines which text is logged to the console when this operator is executed.",
                "",
                false));
        types.add(new ParameterTypeString(
                "longitude field",
                "This parameter defines which text is logged to the console when this operator is executed.",
                "lon",
                false));
        types.add(new ParameterTypeString(
                "latitude field",
                "This parameter defines which text is logged to the console when this operator is executed.",
                "lat",
                false));
        types.add(new ParameterTypeString(
                "identifier field",
                "This parameter defines which text is logged to the console when this operator is executed.",
                "id",
                false));
        types.add(new ParameterTypeString(
                "timestamp field",
                "This parameter defines which text is logged to the console when this operator is executed.",
                "t",
                false));
        types.add(new ParameterTypeInt(
                "expires_ms",
                "This parameter defines which text is logged to the console when this operator is executed.",
                0,
                600000));

        return  types;
    }


}
