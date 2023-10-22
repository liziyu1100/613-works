package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private int field1;
    private int field2;
    private Predicate.Op op;
    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.field1 = field1;
        this.op = op;
        this.field2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        IntField f1 = (IntField) t1.getField(this.field1);
        IntField f2 = (IntField) t2.getField(this.field2);
        if (this.op == Predicate.Op.EQUALS){
            if (f1.equals(f2))return true;
        }
        else if (this.op == Predicate.Op.GREATER_THAN){
            if (f1.greater_than(f2))return true;
        }
        else if (this.op == Predicate.Op.LESS_THAN){
            if (f1.less_than(f2))return true;
        }
        else if (this.op == Predicate.Op.GREATER_THAN_OR_EQ){
            if (f1.equals(f2) || f1.greater_than(f2))return true;
        }
        else if (this.op == Predicate.Op.LESS_THAN_OR_EQ){
            if (f1.equals(f2) || f1.less_than(f2))return true;
        }

        return false;
    }
    
    public int getField1()
    {
        // some code goes here
        return -1;
    }
    
    public int getField2()
    {
        // some code goes here
        return -1;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return null;
    }
}
