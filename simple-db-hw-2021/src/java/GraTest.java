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
        person temp = null;
        try {
            temp.a=5;
            int b = 5;
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
