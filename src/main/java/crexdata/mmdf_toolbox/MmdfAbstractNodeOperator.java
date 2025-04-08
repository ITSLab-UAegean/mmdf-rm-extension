package crexdata.mmdf_toolbox;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.tools.LogService;
import ioobject.KStreamDataContainer;

import java.util.*;
import java.util.logging.Level;

public class MmdfAbstractNodeOperator extends Operator {


    private Stack<MmdfAbstractNodeOperator> operators = new Stack<>();
    public MmdfAbstractNodeOperator(OperatorDescription description) {
        super(description);
    }




    @Override
    public void doWork() throws OperatorException {

        doDefaultWork();
    }


    public void doDefaultWork() throws OperatorException {
        LogService.getRoot().log(Level.INFO,"MMDF testing");
    }
}
