package def_elements;

public class DebugLog {

    public static void log(String message) {
        System.out.println(message);
    }

    public static void logThread(String message) {
        System.out.println(Thread.currentThread().getName() + ": " + message);
    }
}
