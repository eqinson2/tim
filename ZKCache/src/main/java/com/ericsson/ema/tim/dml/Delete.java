package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.dml.predicate.AbstractPredicate;
import com.ericsson.ema.tim.dml.predicate.Eq;
import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ericsson.ema.tim.dml.Select.select;

public class Delete extends AbstractChangeOperator implements Operator {
    private final static Logger LOGGER = LoggerFactory.getLogger(Delete.class);

    private final List<Eq> eqs = new ArrayList<>();

    private Delete(String... fields) {
    }

    public static Delete delete() {
        return new Delete();
    }

    public Delete from(String tab) {
        this.table = tab;
        return this;
    }

    public Delete where(Eq eq) {
        this.eqs.add(eq);
        eq.setOperator(this);
        return this;
    }

    private boolean validateDeleteOperation() {
        List<String> listOfDeleteFields = eqs.stream().map(AbstractPredicate::getField).collect(Collectors.toList());
        Set<String> listOfTableFields = getContext().getTableMetadata().keySet();
        return listOfTableFields.containsAll(listOfDeleteFields) && listOfDeleteFields.containsAll(listOfTableFields);
    }

    private void doExecute() throws KeeperException.ConnectionLossException {
        Selector partSelector = select().from(this.table);
        for (Eq eq : eqs) {
            partSelector = partSelector.where(eq);
        }

        boolean isEmpty = partSelector.collect().isEmpty();

        initExecuteContext();
        if (!validateDeleteOperation())//ensure to delete on row each tme
            throw new DmlBadSyntaxException("Error: Not specify all table fields in delete!");
        this.records = cloneList(this.records);
        if (!isEmpty) {
            this.records.removeIf(r -> eqs.stream().allMatch(eq -> eq.eval(r)));
        } else {
            throw new DmlBadSyntaxException("Error: delete a record which doesn't exists!");
        }
    }

    public synchronized void execute() throws KeeperException.ConnectionLossException {
        doExecute();
        persist(this.records);
    }

    public void executeDebug() throws KeeperException.ConnectionLossException {
        doExecute();
        this.records.forEach(System.out::println);
    }
}
