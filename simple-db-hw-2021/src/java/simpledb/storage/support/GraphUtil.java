package simpledb.storage.support;

import com.sun.org.apache.xpath.internal.operations.Bool;
import simpledb.transaction.TransactionId;

import java.util.*;

public class GraphUtil {
    private Map<String, List<String>>adj;
    private Map<String,Boolean>visit;
    private Object res_lock = new Object();

    public GraphUtil(){
        adj = new HashMap<>();
        visit = new HashMap<>();
    }
    public Map<String, List<String>> adj_copy(){
        Map<String, List<String>> copy_ver = new HashMap<>();
        Iterator<String> iterator = adj.keySet().iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            List<String> value = adj.get(key);
            String newkey = new String(key);
            List<String> newvalue = new ArrayList<>();
            if (value != null) for (int i = 0;i<value.size();i++)newvalue.add(new String(value.get(i)));
            copy_ver.put(newkey,newvalue);
        }
        return copy_ver;
    }
    public Map<String,Boolean>visit_copy(){
        Map<String,Boolean>copy_ver = new HashMap<>();
        for (Map.Entry<String, Boolean>entry: visit.entrySet()){
            String newkey = new String(entry.getKey());
            Boolean newvalue = new Boolean(entry.getValue());
            copy_ver.put(newkey,newvalue);
        }
        return copy_ver;
    }
    public void init_visit(){
        Iterator<String> iterator = visit.keySet().iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            visit.put(key,false);
        }
    }
    public boolean addnode(String src,Iterator<TransactionId> iterator){
        boolean sign = false;
        synchronized (res_lock){
            Map<String, List<String>> copy_ver = adj_copy();
            Map<String,Boolean> new_visit = visit_copy();
            while (iterator.hasNext()){
                String dst = Long.toString(iterator.next().getId());
                if (src.equals(dst))continue;
                List<String>srclist = adj.get(src);
                if (srclist == null){
                    srclist = new ArrayList<>();
                    adj.put(src,srclist);
                    visit.put(src,false);
                }
                if (!srclist.contains(dst)){
                    srclist.add(dst);
                    List<String>dstlist = adj.get(dst);
                    if (dstlist == null){
                        dstlist = new ArrayList<>();
                        adj.put(dst,dstlist);
                    }
                    if (visit.get(dst)==null)visit.put(dst,false);
                }

                if (havaCircle(src)){
                    sign = true;
                    break;
                }
                else{
                    init_visit();
                }
            }
            if (sign){
                adj = copy_ver;
                visit = new_visit;
                return false;
            }
            else{
                return true;
            }
        }
    }
    public boolean havaCircle(String src){
        Queue<List<String>>queue = new LinkedList<>();
        queue.add(adj.get(src));
        visit.put(src,true);
        while (!queue.isEmpty()){
            List<String>temp = queue.poll();
            if (temp == null) continue;
            for (int i =0;i<temp.size();i++){
                if (temp.get(i).equals(src))return true;
                if (!visit.get(temp.get(i))){
                    queue.add(adj.get(temp.get(i)));
                    visit.put(temp.get(i),true);
                }
            }
        }
        return false;
    }
    public void remove(String src){
        synchronized (res_lock){
            adj.remove(src);
//            Iterator<String> iterator = adj.keySet().iterator();
//            while (iterator.hasNext()){
//                String key = iterator.next();
//                adj.get(key).remove(src);
//            }
        }
    }
}
