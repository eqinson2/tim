package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.dml.predicate.Eq;
import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.ericsson.ema.tim.dml.Select.select;

public class Update extends AbstractChangeOperator implements Operator {
    private final static Logger LOGGER = LoggerFactory.getLogger(Update.class);

    private final List<Eq> eqs = new ArrayList<>();
    private List<String[]> updateFields = new ArrayList<>();

    private Update(String... fields) {
    }

    public static Update update() {
        return new Update();
    }

    public Update into(String tab) {
        this.table = tab;
        return this;
    }

    public Update where(Eq eq) {
        this.eqs.add(eq);
        eq.setOperator(this);
        return this;
    }

    public Update set(String field, String value) {
        updateFields.add(new String[]{field, value});
        return this;
    }

    private void doExecute() {
        if (updateFields.isEmpty())
            throw new DmlBadSyntaxException("Error: missing updateFields and addFields!");

        Selector partSelector = select().from(this.table);
        for (Eq eq : eqs) {
            partSelector = partSelector.where(eq);
        }

        boolean isEmpty = partSelector.collect().isEmpty();

        initExecuteContext();
        this.records = cloneList(this.records);
        if (!isEmpty) {
            for (Object obj : this.records) {
                if (eqs.stream().allMatch(eq -> eq.eval(obj))) {
                    for (String[] update : updateFields) {
                        Object newObject = realValue(update);
                        invokeSetByReflection(obj, update[0], newObject);
                    }
                }
            }
        } else {
            throw new DmlBadSyntaxException("Error: Update a record which doesn't exists!");
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
