package simpledb;

import java.io.Serial;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    @Serial
    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private int afield;
    private Op what;
    private final Map<Field, Integer> groupCounts;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupCounts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupVal;
        if (gbfield == NO_GROUPING) {
            groupVal = null;
        } else {
            groupVal = tup.getField(gbfield);
        }
        groupCounts.putIfAbsent(groupVal, 0);
        groupCounts.put(groupVal, groupCounts.get(groupVal) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        TupleDesc tupleDesc;
        if (gbfield == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
        List<Tuple> tuples = new ArrayList<>();

        for (Map.Entry<Field, Integer> en : groupCounts.entrySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            if (gbfield == NO_GROUPING) {
                tuple.setField(0, new IntField(en.getValue()));
            } else {
                tuple.setField(0, en.getKey());
                tuple.setField(1, new IntField(en.getValue()));
            }
            tuples.add(tuple);
        }

        return new TupleIterator(tupleDesc, tuples);
    }
}


