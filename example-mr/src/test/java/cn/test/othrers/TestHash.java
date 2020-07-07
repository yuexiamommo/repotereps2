package cn.test.othrers;

/**
 * Created by Allen Woon
 */
public class TestHash {
    public static void main(String[] args) {
        String a = "hello";
        String b = "hadoop";

        System.out.println(a.hashCode());
        System.out.println(b.hashCode());

        System.out.println(a.hashCode() & Integer.MAX_VALUE);
        System.out.println(b.hashCode() & Integer.MAX_VALUE);

        System.out.println((a.hashCode() & Integer.MAX_VALUE) % 2);
        System.out.println((b.hashCode() & Integer.MAX_VALUE) % 2);
    }
}
