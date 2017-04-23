package com.ericsson.ema.tim.dml;

import com.ericsson.ema.tim.dml.group.GroupBy;
import com.ericsson.ema.tim.dml.order.OrderBy;
import com.ericsson.ema.tim.dml.predicate.Predicate;
import com.ericsson.ema.tim.reflection.MethodInvocationCache;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ericsson.ema.tim.dml.TableInfoMap.tableInfoMap;
import static com.ericsson.ema.tim.lock.ZKCacheRWLockMap.zkCacheRWLock;
import static com.ericsson.ema.tim.reflection.MethodInvocationCache.AccessType.GET;
import static com.ericsson.ema.tim.reflection.Tab2MethodInvocationCacheMap.tab2MethodInvocationCacheMap;
import static java.util.stream.Collectors.groupingBy;

public class Select implements Selector {
    private final static String TUPLE_FIELD = "records";

    private final List<String> selectedFields;
    private final List<Predicate> predicates = new ArrayList<>();
    private final List<OrderBy> orderBys = new ArrayList<>();
    private GroupBy groupBy;
    private int limit = Integer.MIN_VALUE;
    private int skip = Integer.MIN_VALUE;

    private String table;
    private TableInfoContext context;
    private List<Object> records;
    private MethodInvocationCache methodInvocationCache;

    private Select() {
        this.selectedFields = Collections.emptyList();
    }

    private Select(String... fields) {
        this.selectedFields = (fields == null || fields.length == 0) ? Collections.emptyList() : Arrays.asList(fields);
    }

    public static Selector select() {
        return new Select();
    }

    public static Selector select(String... fields) {
        return new Select(fields);
    }

    public List<String> getSelectedFields() {
        return Collections.unmodifiableList(selectedFields);
    }

    public MethodInvocationCache getMethodInvocationCache() {
        return methodInvocationCache;
    }

    public TableInfoContext getContext() {
        return context;
    }

    @Override
    public Selector from(String tab) {
        this.table = tab;
        return this;
    }

    @Override
    public Selector where(Predicate predicate) {
        this.predicates.add(predicate);
        ((SelectClause) predicate).setSelector(this);
        return this;
    }

    @Override
    public Selector orderBy(String field, String asc) {
        OrderBy o = OrderBy.orderby(field, asc);
        this.orderBys.add(o);
        o.setSelector(this);
        return this;
    }

    @Override
    public Selector orderBy(String field) {
        OrderBy o = OrderBy.orderby(field);
        this.orderBys.add(o);
        o.setSelector(this);
        return this;
    }

    @Override
    public Selector groupBy(String field) {
        if (groupBy != null)
            throw new DmlBadSyntaxException("Error: only support one groupBy Clause");

        GroupBy g = GroupBy.groupBy(field);
        this.groupBy = g;
        g.setSelector(this);
        return this;
    }

    @Override
    public Selector limit(int limit) {
        if (limit <= 0)
            throw new DmlBadSyntaxException("Error: limit must be > 0");
        this.limit = limit;
        return this;
    }

    @Override
    public Selector skip(int skip) {
        if (skip <= 0)
            throw new DmlBadSyntaxException("Error: skip must be > 0");
        this.skip = skip;
        return this;
    }


    private Object invokeGetByReflection(Object obj, String wantedField) {
        Method getter = methodInvocationCache.get(obj.getClass(), wantedField, GET);
        try {
            return getter.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new DmlBadSyntaxException(e.getMessage());//should never happen
        }
    }

    private void initExecuteContext() {
        this.context = tableInfoMap.lookup(table).orElseThrow(() -> new DmlBadSyntaxException("Error: Selecting a " +
            "non-existing table:" + table));
        this.methodInvocationCache = tab2MethodInvocationCacheMap.lookup(table);

        //it is safe because records must be List according to JavaBean definition
        Object tupleField = invokeGetByReflection(context.getTabledata(), TUPLE_FIELD);
        assert (tupleField instanceof List<?>);
        //noinspection unchecked
        this.records = (List<Object>) tupleField;
    }

    /**
     * rwlock table when internalExecute
     *
     * @return List of tuple
     */
    private List<Object> internalExecute() {
        zkCacheRWLock.readLockTable(table);
        try {
            initExecuteContext();
            Stream<Object> stream = records.stream().filter(internalPredicate());
            Optional<Comparator<Object>> c = orderBys.stream().map(OrderBy::comparing).reduce(Comparator::thenComparing);
            if (c.isPresent()) {
                stream = stream.sorted(c.get());
            }
            if (skip > 0) {
                stream = stream.skip(skip);
            }
            if (limit > 0) {
                stream = stream.limit(limit);
            }
            return stream.collect(Collectors.toList());
        } finally {
            zkCacheRWLock.readUnLockTable(table);
        }
    }


    @Override
    public List<Object> collect() {
        if (!selectedFields.isEmpty())
            throw new DmlBadSyntaxException("Error: must use collectBySelectFields if some fields are to be selected");

        return internalExecute();
    }

    @Override
    public List<List<Object>> collectBySelectFields() {
        if (selectedFields.isEmpty())
            throw new DmlBadSyntaxException("Error: Must use execute if full fields are to be selected");

        List<List<Object>> selectedResult = new ArrayList<>();
        for (Object obj : internalExecute()) {
            selectedResult.add(selectedFields.stream().map(field ->
                invokeGetByReflection(obj, field)).collect(Collectors.toList()));
        }
        return selectedResult;
    }

    @Override
    public Map<Object, List<Object>> collectByGroup() {
        if (!this.getSelectedFields().isEmpty())
            throw new DmlBadSyntaxException("Error: should not specify selected fields when groupBy");

        zkCacheRWLock.readLockTable(table);
        try {
            initExecuteContext();
            Stream<Object> stream = records.stream().filter(internalPredicate());
            Optional<Comparator<Object>> c = orderBys.stream().map(OrderBy::comparing).reduce(Comparator::thenComparing);
            if (c.isPresent()) {
                stream = stream.sorted(c.get());
            }
            if (skip > 0) {
                stream = stream.skip(skip);
            }
            if (limit > 0) {
                stream = stream.limit(limit);
            }
            if (groupBy != null) {
                return stream.collect(groupingBy(groupBy.grouping()));
            } else {
                throw new DmlBadSyntaxException("Error: must specify groupBy when using collectByGroup.");
            }
        } finally {
            zkCacheRWLock.readUnLockTable(table);
        }
    }

    @Override
    public long count() {
        if (limit != Integer.MIN_VALUE || skip != Integer.MIN_VALUE)
            throw new DmlBadSyntaxException("Error: meaningless to specify skip/limit in count.");

        zkCacheRWLock.readLockTable(table);
        try {
            initExecuteContext();
            return records.stream().filter(internalPredicate()).count();
        } finally {
            zkCacheRWLock.readUnLockTable(table);
        }
    }

    @Override
    public boolean exists() {
        if (limit != Integer.MIN_VALUE || skip != Integer.MIN_VALUE)
            throw new DmlBadSyntaxException("Error: meaningless to specify skip/limit in exists.");

        zkCacheRWLock.readLockTable(table);
        try {
            initExecuteContext();
            return records.stream().anyMatch(internalPredicate());
        } finally {
            zkCacheRWLock.readUnLockTable(table);
        }
    }

    private java.util.function.Predicate<Object> internalPredicate() {
        return r -> predicates.stream().map(c -> c.eval(r)).reduce(true, Boolean::logicalAnd);
    }
}

