
import crexdata.mmdf.operator.MmdfFusionNodeOperator;


import java.io.IOException;

public class FusionApp {

    public static void main(String[] args) throws IOException {

        MmdfFusionNodeOperator operator = new MmdfFusionNodeOperator(null);

        try {
            operator.doDefaultWork();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

}
