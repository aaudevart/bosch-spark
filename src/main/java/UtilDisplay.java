import java.util.List;
import java.util.Map;

/**
 * Created by alexia on 02/09/2016.
 */
public class UtilDisplay {


    public static void displayMapStringList(Map<String, List<Integer>> map) {
        for (Map.Entry<String, List<Integer>> entry : map.entrySet())
        {
            System.out.print(entry.getKey() + "/" + entry.getValue());
           /* for (int i = 0 ; i  entry.getValue()) {
                System.out.print(val);
            }*/
            System.out.println();
        }
    }

    public void displayMapMapIntList(Map<Integer, List<String>> map) {
        for (Map.Entry<Integer, List<String>> entry : map.entrySet())
        {
            System.out.print(entry.getKey() + "/");
            for (String val : entry.getValue()) {
                System.out.print(val);
            }
            System.out.println();
        }
    }

    public static void displayMapIntString(Map<Integer, String> map) {
        for (Map.Entry<Integer, String> entry : map.entrySet())
        {
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }
    }
}
