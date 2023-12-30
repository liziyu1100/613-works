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
    private Object read_lock = new Object();
    private Object write_lock = new Object();
    public Tranlock(){
        sh_locks = new HashSet<>();
        ex_lock = null;
    }
    public void lock(TransactionId tid, Permissions perm) throws InterruptedException, TransactionAbortedException {
        if (perm == Permissions.READ_ONLY){
            boolean sign;
            synchronized (res_lock){
                isDeadLock(tid,perm);
                if (ex_lock == null){
                    sh_locks.add(tid);
                    sign = true;
                }
                else if (ex_lock == tid){
                    ex_lock = null;
                    sh_locks.add(tid);
                    sign = true;
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
                }
                else if ((sh_locks.size() == 0 && ex_lock == null)||(ex_lock != null && ex_lock.equals(tid))){
                    ex_lock = tid;
                    sign = 0;
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
                    read_lock.wait();
                }
                synchronized (res_lock){
//                    System.out.println("升级成功");
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
                        if (sh_locks.size() == 1)read_lock.notify();
                    }
                }
            }
            //System.out.println(tid+" 移除完毕 当前剩余"+sh_locks.size());
        }
    }
}