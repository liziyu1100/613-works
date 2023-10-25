package simpledb.execution;

import org.omg.CORBA.PRIVATE_MEMBER;
import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbftype;
    private int afield;
    private Op what;
    private List<Field> gblist;
    private List<Field> alist;
    private List<Integer>gr_num;
    private TupleDesc td;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.afield = afield;
        this.gbfield = gbfield;
        this.what = what;
        this.gbftype = gbfieldtype;
        this.gblist = new ArrayList<>();
        this.gr_num = new ArrayList<>();
        this.alist = new ArrayList<>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        this.td = tup.getTupleDesc();
        if (this.gbfield != -1){
            boolean sign = false;
            int index = 0;
            for (int i =0;i<this.gblist.size();i++){
                if (gblist.get(i).equals(tup.getField(this.gbfield))){
                    sign = true;
                    index = i;
                    break;
                }
            }
            if (sign == false){
                this.gblist.add(tup.getField(this.gbfield));
                if (this.what == Op.COUNT){
                    this.alist.add(new IntField(1));
                }
                this.gr_num.add(1);
            }
            else{
                if (this.what == Op.COUNT){
                    int cur_value = ((IntField)this.alist.get(index)).getValue();
                    int tup_value = 1;
                    cur_value = cur_value + tup_value;
                    this.alist.set(index,new IntField(cur_value));
                }
                else if (this.what == Op.SUM){
                    int cur_value = ((IntField)this.alist.get(index)).getValue();
                    int tup_value = ((IntField)tup.getField(this.afield)).getValue();
                    this.alist.set(index,new IntField(cur_value+tup_value));
                }
                this.gr_num.set(index,this.gr_num.get(index)+1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");
        if (this.gbfield == -1){
//            List<Tuple> tups = new ArrayList<>();
//            Type []types = {Type.STRING_TYPE};
//            TupleDesc ttd = new TupleDesc(types);
//            for (int i =0;i<this.alist.size();i++){
//                Tuple tup=new Tuple(ttd);
//                tup.setField(0,this.alist.get(i));
//                tups.add(tup);
//            }
//            return new TupleIterator(ttd,tups);
            throw new UnsupportedOperationException("please implement me for StringAggr_no_gbfield");
        }
        else{
            List<Tuple> tups = new ArrayList<>();
            Type []types = {this.gbftype,Type.INT_TYPE};
            TupleDesc ttd = new TupleDesc(types);
            for (int i =0;i<this.gblist.size();i++){
                Tuple tup=new Tuple(ttd);
                tup.setField(1, new IntField(this.gr_num.get(i)));
                tup.setField(0,this.gblist.get(i));
                tups.add(tup);
            }
            return new TupleIterator(ttd, tups);
        }
    }

}
