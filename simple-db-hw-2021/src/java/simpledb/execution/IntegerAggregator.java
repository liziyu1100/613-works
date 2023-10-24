package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private List<Field> gflist;
    private List<Field> alist;
    private TupleDesc cur_td;


    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.alist = new ArrayList<>();
        if (gbfield != -1){
            this.gflist = new ArrayList<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        this.cur_td = tup.getTupleDesc();
        if (this.gbfield != -1){
            boolean sign = false;
            int index = 0;
            for (int i =0;i<gflist.size();i++){
                if (gflist.get(i).equals(tup.getField(this.gbfield))){
                    sign = true;
                    index = i;
                    break;
                }
            }
            if (sign == false){
                this.gflist.add(tup.getField(this.gbfield));
                this.alist.add(tup.getField(this.afield));
            }
            else{
                if (this.what == Op.SUM){
                    int value = ((IntField)this.alist.get(index)).getValue()+((IntField)tup.getField(this.afield)).getValue();
                    this.alist.set(index,new IntField(value));
                }
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");
        if (this.gbfield == -1){
            List<Tuple> tups = new ArrayList<>();
            Type []types = {Type.INT_TYPE};
            TupleDesc ttd = new TupleDesc(types);
            for (int i =0;i<this.alist.size();i++){
                Tuple tup=new Tuple(ttd);
                tup.setField(0,this.alist.get(i));
                tups.add(tup);
            }
            return new TupleIterator(ttd,tups);
        }
        else{
            List<Tuple> tups = new ArrayList<>();
            Type []types = {this.gbfieldtype,Type.INT_TYPE};
            TupleDesc ttd = new TupleDesc(types);
            for (int i =0;i<this.alist.size();i++){
                Tuple tup=new Tuple(ttd);
                tup.setField(1,this.alist.get(i));
                tup.setField(0,this.gflist.get(i));
                tups.add(tup);
            }
            return new TupleIterator(ttd, tups);
        }
    }

}
