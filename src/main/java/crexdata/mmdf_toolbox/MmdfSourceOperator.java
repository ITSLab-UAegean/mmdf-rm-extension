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
