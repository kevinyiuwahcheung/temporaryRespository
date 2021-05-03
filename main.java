import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class main {
    public static void main(String[] args) {
//        String content = "" +
//                "apple apple apple\n" +
//                "banana\n" +
//                "orange orange\n";
//
//        StringTokenizer st = new StringTokenizer(content.toString());
//        while(st.hasMoreTokens()){
//            System.out.println(st.nextToken());
//        }
//        System.out.println(""+5/2);
        DecimalFormat df = new DecimalFormat("###.####");
        System.out.println( df.format((double)5/2) );
    }
}
