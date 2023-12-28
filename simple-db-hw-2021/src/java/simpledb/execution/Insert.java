package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator child;
    private int tableId;
    private int in_num;
    private Iterator<Tuple>res_it;
    private boolean insert_finish = false;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.child=child;
        this.transactionId = t;
        this.tableId = tableId;
        in_num = 0;
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))throw new DbException("TupleDesc not match");
    }
    private void init() throws DbException,TransactionAbortedException{
        if (insert_finish == false){
            try{
                child.open();
                while (child.hasNext()){
                    Tuple t = child.next();
                    Database.getBufferPool().insertTuple(this.transactionId,tableId,t);
                    in_num = in_num +1;
                }
                child.close();
                List<Tuple> res = new ArrayList<>();
                Type[] types = {Type.INT_TYPE};
                Tuple t =  new Tuple(new TupleDesc(types));
                t.setField(0,new IntField(in_num));
                res.add(t);
                res_it = res.iterator();
                insert_finish = true;
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        init();
        if (res_it.hasNext())return res_it.next();
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }
}
