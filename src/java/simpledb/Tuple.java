package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private Field[] fields;
    private TupleDesc tupleDescs;
    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.tupleDescs = td;
        this.fields = new Field[td.numFields()];
        this.recordId = null;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDescs;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if (i < 0 || i >= tupleDescs.numFields()) {
            throw new NoSuchElementException("Field index is out of bounds");
        }
        fields[i] = f;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        if (i < 0 || i >= tupleDescs.numFields()) {
            throw new NoSuchElementException("Field index is out of bounds");
        }
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append(fields[i].toString());
        }
        return sb.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        return Arrays.stream(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        this.tupleDescs = td;
    }

    /**
     * Merges two tuples.
     *
     * @param t1 tuple one
     * @param t2 tuple two
     * @return A new merged tuple who have tuples of t1 and t2
     */
    public static Tuple merge(Tuple t1, Tuple t2) {
        TupleDesc mergedtupleDesc = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());
        Tuple mergedtuple = new Tuple(mergedtupleDesc);

        int index = 0;

        for (Field field1 : t1.fields) {
            mergedtuple.setField(index, field1);
            index++;
        }

        for (Field field2 : t2.fields) {
            mergedtuple.setField(index, field2);
            index++;
        }

        return mergedtuple;
    }
}
