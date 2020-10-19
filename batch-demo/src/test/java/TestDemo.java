public class TestDemo {

    public static void main(String[] args) {
        String str = "a$_b$_c";
        String[] $_s = str.split("\\$_");
        System.out.println($_s.length);
    }

}
