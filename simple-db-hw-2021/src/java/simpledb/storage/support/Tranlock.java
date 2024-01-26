package simpledb.storage.support;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;

public class Tranlock{
    private Set<TransactionId> sh_locks;
    private TransactionId ex_lock;
    private Object res_lock = new Object();
    private static GraphUtil graphUtil = new GraphUtil();
    private List<TransactionId> for_read_q;
    private Object read_lock = new Object();
    private Object write_lock = new Object();
    private Object for_lock = new Object();
    public Tranlock(){
        sh_locks = new HashSet<>();
        ex_lock = null;
        for_read_q = new ArrayList<>();
    }
    public boolean islocked(){
        synchronized (res_lock){
            if (ex_lock == null && sh_locks.size() == 0)
                return false;
            else
                return true;
        }
    }
    public void lock(TransactionId tid, Permissions perm) throws InterruptedException, TransactionAbortedException {
        if (perm == Permissions.READ_ONLY){
            boolean sign;
            synchronized (res_lock){
                List<TransactionId>dstit = new ArrayList<>();
                dstit.add(tid);
                synchronized (for_lock){
                    for (int i =0;i<for_read_q.size();i++){
                        graphUtil.addnode(Long.toString(for_read_q.get(i).getId()),dstit.iterator());
                    }
                }
                isDeadLock(tid,perm);
                if (ex_lock == null){
                    sign = true;
                    if (!sh_locks.contains(tid)){
                        sh_locks.add(tid);
                        //System.out.println(tid.getId()+ "  get read lock");
                    }
                }
                else if (ex_lock == tid){
                    sign = true;
                    //System.out.println(tid.getId()+ "  have got read_write_lock");
                }
                else{
                    sign = false;
                }
            }
            if (!sign){
                synchronized (write_lock){
                    write_lock.wait();
                }
                synchronized (res_lock){
                    sh_locks.add(tid);
                }
            }

        }
        else if (perm == Permissions.READ_WRITE){
            int sign = 0;
            synchronized (res_lock){
                isDeadLock(tid,perm);
                if (sh_locks.size()==1 && sh_locks.contains(tid)){
                    sh_locks.remove(tid);
                    ex_lock = tid;
                    sign = 0;
                    //System.out.println(tid.getId()+ "  get write lock");
                }
                else if ((sh_locks.size() == 0 && ex_lock == null)||(ex_lock != null && ex_lock.equals(tid))){
                    ex_lock = tid;
                    sign = 0;
                    //System.out.println(tid.getId()+ "  get write lock");
                }
                else if (sh_locks.size()>1 || (sh_locks.size()==1&&!sh_locks.contains(tid))){
                    sign = 1;
                }
                else if (ex_lock != null && !ex_lock.equals(tid)){
                    sign = 2;
                }
            }
            if (sign == 1){
                synchronized (read_lock){
                    synchronized (for_lock){
                        for_read_q.add(tid);
                    }
                    //System.out.println(tid.getId()+ "  read_lock block");
                    read_lock.wait();
                }
                synchronized (res_lock){
                    synchronized (for_lock){
                        for_read_q.remove(tid);
                    }
                    sh_locks.remove(tid);
                    ex_lock = tid;
                }
            }
            else if (sign == 2){
                synchronized (write_lock){
                    write_lock.wait();
                }
                synchronized (res_lock){
                    sh_locks.remove(tid);
                    ex_lock = tid;
                }
            }

        }
    }
    public void isDeadLock(TransactionId tid, Permissions perm) throws TransactionAbortedException {
        if (ex_lock!=null && !ex_lock.equals(tid)){
            List<TransactionId> temp = new ArrayList<>();
            temp.add(ex_lock);
            Iterator<TransactionId>iterator = temp.iterator();
            if (!graphUtil.addnode(Long.toString(tid.getId()),iterator))
                throw new TransactionAbortedException();
        }
        else if (sh_locks!=null && perm==Permissions.READ_WRITE){
            Iterator<TransactionId> iterator = sh_locks.iterator();
            if (!graphUtil.addnode(Long.toString(tid.getId()),iterator))
                        throw new TransactionAbortedException();
        }
    }
    public void unlock(TransactionId tid){
        synchronized (res_lock){
            if (ex_lock == tid){
                graphUtil.remove(Long.toString(tid.getId()));
                ex_lock = null;
                synchronized (write_lock){
                    write_lock.notify();
                }
            }
            else{
                if (sh_locks.remove(tid)){
                    graphUtil.remove(Long.toString(tid.getId()));
                    synchronized (read_lock){
                        synchronized (res_lock){
                            if (sh_locks.size() == 1){
                                synchronized (for_lock){
                                    if (for_read_q.size()==1 && sh_locks.iterator().next().equals(for_read_q.get(0)))read_lock.notify();
                                }

                            }
                            else if (sh_locks.size() == 0)read_lock.notify();
                        }
                    }
                }
            }
            //System.out.println(tid+" 移除完毕 当前剩余"+sh_locks.size());
        }
    }
}