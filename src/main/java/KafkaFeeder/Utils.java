package KafkaFeeder;


public class Utils {
    public static void showNSymbol(String symbol, int n){
        for(int i = 0; i < n; ++i) 
            System.out.print("#");
        System.out.println();
    }
}