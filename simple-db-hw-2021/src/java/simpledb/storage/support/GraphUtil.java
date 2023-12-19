package simpledb.storage.support;

import java.util.*;

public class GraphUtil {
    private Map<String, List<String>>adj;
    private Map<String,Boolean>visit;

    public static void main(String[] args) {
        GraphUtil graph = new GraphUtil();
        System.out.println(graph.addnode("1","2"));
        System.out.println(graph.addnode("3","4"));
        System.out.println(graph.addnode("2","3"));
        System.out.println(graph.addnode("1","4"));
        System.out.println(graph.addnode("4","2"));

    }
    public GraphUtil(){
        adj = new HashMap<>();
        visit = new HashMap<>();
    }
    public void init_visit(){
        Iterator<String> iterator = visit.keySet().iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            visit.put(key,false);
        }
    }
    public boolean addnode(String src,String dst){
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
                visit.put(dst,false);
            }
        }

        if (havaCircle(src) == true){
            srclist.remove(dst);
            init_visit();
            return false;
        }
        else{
            init_visit();
            return true;
        }
    }
    public boolean havaCircle(String src){
        Queue<List<String>>queue = new LinkedList<>();
        queue.add(adj.get(src));
        visit.put(src,true);
        while (!queue.isEmpty()){
            List<String>temp = queue.poll();
            for (int i =0;i<temp.size();i++){
                if (temp.get(i).equals(src))return true;
                if (visit.get(temp.get(i)) == false){
                    queue.add(adj.get(temp.get(i)));
                    visit.put(temp.get(i),true);
                }
                else{
                    continue;
                }
            }
        }
        return false;
    }
    public void remove(String src){
        adj.remove(src);
        Iterator<String> iterator = adj.keySet().iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            adj.get(key).remove(src);
        }
    }
}
