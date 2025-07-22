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
        LogService.getRoot().log(Level.INFO,"MMDF Abstract node");
    }
}
