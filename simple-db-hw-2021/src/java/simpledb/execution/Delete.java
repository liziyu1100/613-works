package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator child;
    private Iterator<Tuple>res_it;
    private int del_num;
    private boolean delete_finish = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.child=child;
        this.transactionId = t;
        del_num = 0;
    }
    private void init() throws DbException, TransactionAbortedException {
        if (delete_finish == false){
            try{
                child.open();
                List<Tuple>del = new ArrayList<>();
                while (child.hasNext()){
                    Tuple t = child.next();
                    del.add(t);
                    del_num = del_num +1;
                }
                for (int i =0;i<del.size();i++){
                    Database.getBufferPool().deleteTuple(this.transactionId,del.get(i));
                }
                del = null;
                child.close();
            }catch (IOException e){
                e.printStackTrace();
            }
            List<Tuple> res = new ArrayList<>();
            Type[] types = {Type.INT_TYPE};
            Tuple t =  new Tuple(new TupleDesc(types));
            t.setField(0,new IntField(del_num));
            res.add(t);
            res_it = res.iterator();
            delete_finish = true;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        init();
        if (this.res_it.hasNext())return res_it.next();
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
