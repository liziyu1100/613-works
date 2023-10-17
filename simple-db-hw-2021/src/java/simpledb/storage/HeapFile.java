package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File df;
    private TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.df = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.df;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.df.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        FileInputStream fis=null;
        byte[] resultBytes=new byte[BufferPool.getPageSize()];
        int pgn = pid.getPageNumber();
        int i = 0;
        boolean sign = false;
        try{
            fis=new FileInputStream(this.df);
            int len;
            while((len=fis.read(resultBytes))!=-1){
                if (i == pgn-1){
                    sign = true;
                    break;
                }
                i++;
            }
            HeapPage hp = new HeapPage((HeapPageId) pid,resultBytes);
            if (sign == true){

                return hp;
            }
            return null;
        }catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        if (this.df.length()%BufferPool.getPageSize()==0)return (int) (this.df.length()/BufferPool.getPageSize());
        return (int) (this.df.length()/BufferPool.getPageSize()+1);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(this);
    }
    public class HeapFileIterator implements DbFileIterator  {
        private boolean open = false;
        private HeapFile hf;
        private HeapPageId cur_pageid;
        private Iterator<Tuple> cur_it;
        private int page_num;
        public HeapFileIterator(HeapFile heapFile){
            this.hf = heapFile;
            cur_pageid = new HeapPageId(hf.getId(),1);
            page_num = 0;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (open == false)return false;
            if (next == null) next = readNext();
            return next != null;
        }
        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if (open == false)throw new NoSuchElementException("error,not open");
            if (next == null) {
                next = readNext();
                if (next == null) throw new NoSuchElementException();
            }

            Tuple result = next;
            next = null;
            return result;
        }

        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (this.cur_it == null){
                HeapPage hp = (HeapPage) Database.getBufferPool().getPage(null,this.cur_pageid,null);
                if (hp == null){
                    hp = (HeapPage) hf.readPage(cur_pageid);
                    Database.getBufferPool().addPage(null,hp);
                }
                cur_it = hp.iterator();
                page_num = page_num+1;
            }
            if (this.cur_it.hasNext()){
                return cur_it.next();
            }
            else{
                if ((page_num+1)*BufferPool.getPageSize()>hf.getFile().length())return null;
                cur_pageid = new HeapPageId(cur_pageid.getTableId(), cur_pageid.getPageNumber()+1);
                HeapPage hp = (HeapPage) Database.getBufferPool().getPage(null,this.cur_pageid,null);
                if (hp == null){
                    hp = (HeapPage) hf.readPage(cur_pageid);
                    Database.getBufferPool().addPage(null,hp);
                }
                cur_it = hp.iterator();
                page_num = page_num+1;
                return readNext();
            }
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            open=true;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.cur_it=null;
            this.page_num=0;
        }
        @Override
        public void close(){
            open = false;
        }
        private Tuple next = null;
    }

}

