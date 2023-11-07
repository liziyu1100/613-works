import java.util.LinkedHashMap;
import java.util.Map;

public class GraTest {
    class person{
        private int a;
        private int b;
        public person(int a,int b){
            this.a= a;
            this.b= b;
        }
    }
    public static void main(String[] args) {
        LinkedHashMap<Integer, String> hm = new LinkedHashMap<>();

        hm.put(1, "深圳");
        hm.put(2, "广州");
        hm.put(3, "惠州");
        hm.put(4, "扬州");

        System.out.println("LinkedHashMap 集合" + hm);

        for (Map.Entry<Integer, String> mapElement :
                hm.entrySet()) {
            // 获取键
            Integer key = mapElement.getKey();

            // 获取值
            String value = mapElement.getValue();

            hm.remove(key);
            break;
        }
        System.out.println("LinkedHashMap 集合" + hm);
    }
}
