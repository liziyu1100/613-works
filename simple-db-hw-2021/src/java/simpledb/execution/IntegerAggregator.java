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
    private List<Integer> grp_num;
    private List<Integer>grp_sum;


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
        this.grp_num = new ArrayList<>();
        this.grp_sum = new ArrayList<>();
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
                if (this.what == Op.COUNT){
                    this.alist.add(new IntField(1));
                }
                else{
                    this.alist.add(tup.getField(this.afield));
                }
                this.grp_num.add(1);
                this.grp_sum.add(((IntField)tup.getField(this.afield)).getValue());
            }
            else{
                if (this.what == Op.SUM){
                    int value = ((IntField)this.alist.get(index)).getValue()+((IntField)tup.getField(this.afield)).getValue();
                    this.alist.set(index,new IntField(value));
                }
                else if (this.what == Op.MIN){
                    int cur_value = ((IntField)this.alist.get(index)).getValue();
                    int tup_value = ((IntField)tup.getField(this.afield)).getValue();
                    if (tup_value < cur_value) {
                        this.alist.set(index,new IntField(tup_value));
                    }
                }
                else if (this.what == Op.MAX){
                    int cur_value = ((IntField)this.alist.get(index)).getValue();
                    int tup_value = ((IntField)tup.getField(this.afield)).getValue();
                    if (tup_value > cur_value) {
                        this.alist.set(index,new IntField(tup_value));
                    }
                }
                else if (this.what == Op.AVG){
                    int cur_value = this.grp_sum.get(index);
                    int tup_value = ((IntField)tup.getField(this.afield)).getValue();
                    cur_value = (cur_value +tup_value)/(this.grp_num.get(index)+1);
                    this.alist.set(index,new IntField(cur_value));
                }
                else if (this.what == Op.COUNT){
                    int cur_value = ((IntField)this.alist.get(index)).getValue();
                    int tup_value = 1;
                    cur_value = cur_value + tup_value;
                    this.alist.set(index,new IntField(cur_value));
                }
                this.grp_num.set(index,this.grp_num.get(index)+1);
                this.grp_sum.set(index,this.grp_sum.get(index)+((IntField)tup.getField(this.afield)).getValue());
            }
        }
        else{
            int index = 0;
            if (this.grp_sum.size()==0){
                this.grp_sum.add(0);
                this.grp_num.add(0);
                if (this.what == Op.COUNT){
                    this.alist.add(new IntField(1));
                }
                else{
                    this.alist.add(tup.getField(this.afield));
                }
                return;
            }
            if (this.what == Op.SUM){
                int value = ((IntField)this.alist.get(index)).getValue()+((IntField)tup.getField(this.afield)).getValue();
                this.alist.set(index,new IntField(value));
            }
            else if (this.what == Op.MIN){
                int cur_value = ((IntField)this.alist.get(index)).getValue();
                int tup_value = ((IntField)tup.getField(this.afield)).getValue();
                if (tup_value < cur_value) {
                    this.alist.set(index,new IntField(tup_value));
                }
            }
            else if (this.what == Op.MAX){
                int cur_value = ((IntField)this.alist.get(index)).getValue();
                int tup_value = ((IntField)tup.getField(this.afield)).getValue();
                if (tup_value > cur_value) {
                    this.alist.set(index,new IntField(tup_value));
                }
            }
            else if (this.what == Op.AVG){
                int cur_value = this.grp_sum.get(index);
                int tup_value = ((IntField)tup.getField(this.afield)).getValue();
                cur_value = (cur_value +tup_value)/(this.grp_num.get(index)+1);
                this.alist.set(index,new IntField(cur_value));
            }
            else if (this.what == Op.COUNT){
                int cur_value = ((IntField)this.alist.get(index)).getValue();
                int tup_value = 1;
                cur_value = cur_value + tup_value;
                this.alist.set(index,new IntField(cur_value));
            }
            this.grp_num.set(index,this.grp_num.get(index)+1);
            this.grp_sum.set(index,this.grp_sum.get(index)+((IntField)tup.getField(this.afield)).getValue());
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
