package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.dml.predicate.Eq;
import com.ericsson.ema.tim.exception.DmlBadSyntaxException;
import com.ericsson.ema.tim.pojo.PojoGenerator;
import com.ericsson.ema.tim.reflection.JavaBeanReflectionProxy;
import com.ericsson.ema.tim.reflection.TabDataLoader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ericsson.ema.tim.dml.Select.select;
import static com.ericsson.ema.tim.dml.predicate.Eq.eq;
import static com.ericsson.ema.tim.reflection.Tab2ClzMap.tab2ClzMap;

public class Insert extends AbstractChangeOperator implements Operator {
    private final static Logger LOGGER = LoggerFactory.getLogger(Insert.class);
    private List<String[]> addFields = new ArrayList<>();

    private Insert(String... fields) {
    }

    public static Insert insert() {
        return new Insert();
    }


    public Insert into(String tab) {
        this.table = tab;
        return this;
    }

    public Insert add(String field, String value) {
        addFields.add(new String[]{field, value});
        return this;
    }

    private boolean validateAddOperation() {
        List<String> listOfUpdateFields = addFields.stream().map(f -> f[0]).collect(Collectors.toList());
        Set<String> listOfTableFields = getContext().getTableMetadata().keySet();
        return listOfTableFields.containsAll(listOfUpdateFields) && listOfUpdateFields.containsAll(listOfTableFields);
    }

    private Object makeNewRecord() {
        try {
            Object tuple;
            Class<?> clz = tab2ClzMap.lookup(this.table)
                    .orElse(Thread.currentThread().getContextClassLoader().loadClass(PojoGenerator.pojoPkg + "." + this.table));
            Object obj = clz.newInstance();
            JavaBeanReflectionProxy proxy = new JavaBeanReflectionProxy(obj);
            tuple = proxy.getTupleListType().newInstance();
            final Object newTuple = tuple;
            addFields.forEach(f -> {
                try {
                    TabDataLoader.fillInField(newTuple, f[0], realValue(f));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            return newTuple;
        } catch (Exception e) {
            return null;
        }
    }

    private void doExecute() {
        if (addFields.isEmpty())
            throw new DmlBadSyntaxException("Error: missing addFields!");

        Selector partSelector = select().from(this.table);
        for (String[] field : addFields) {
            Eq eq = eq(field[0], field[1]);
            partSelector = partSelector.where(eq);
        }

        boolean isEmpty = partSelector.collect().isEmpty();

        initExecuteContext();
        this.records = cloneList(this.records);
        if (isEmpty) {
            if (!validateAddOperation())
                throw new DmlBadSyntaxException("Error: Not add all table fields");

            this.records.add(makeNewRecord());
        } else {
            throw new DmlBadSyntaxException("Error: Insert a record which already exists!");
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
