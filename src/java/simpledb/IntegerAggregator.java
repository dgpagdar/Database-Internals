package simpledb;

import java.io.Serial;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    @Serial
    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<Field, Integer> valueGroupBy;
    private final Map<Field, Integer> countGroupBy;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.valueGroupBy = new HashMap<>();
        this.countGroupBy = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField;

        if (gbfield == NO_GROUPING) {
            groupField = new IntField(-1);
        } else {
            groupField = tup.getField(gbfield);
        }

        countGroupBy.putIfAbsent(groupField, 0);
        int currCount = countGroupBy.get(groupField) + 1;
        countGroupBy.put(groupField, currCount);

        if (what == Op.COUNT) {
            valueGroupBy.put(groupField, currCount);
        } else {
            IntField currAggField = (IntField) tup.getField(afield);
            int value = currAggField.getValue();

            valueGroupBy.putIfAbsent(groupField, (what == Op.MIN) ? Integer.MAX_VALUE : 0);
            int currValue = valueGroupBy.get(groupField);

            switch (what) {
                case MIN:
                    valueGroupBy.put(groupField, Math.min(currValue, value));
                    break;
                case MAX:
                    valueGroupBy.put(groupField, Math.max(currValue, value));
                    break;
                case SUM:
                case AVG:
                    valueGroupBy.put(groupField, currValue + value);
                    break;
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        TupleDesc tupleDesc;
        if (gbfield == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        List<Tuple> tuples = new ArrayList<>();
        for (Field value : valueGroupBy.keySet()) {
            int aggValue = valueGroupBy.get(value);

            if (what == Op.AVG) {
                aggValue /= countGroupBy.get(value);
            }

            Tuple tuple = new Tuple(tupleDesc);
            if (gbfield == Aggregator.NO_GROUPING) {
                tuple.setField(0, new IntField(aggValue));
            } else {
                tuple.setField(0, value);
                tuple.setField(1, new IntField(aggValue));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
