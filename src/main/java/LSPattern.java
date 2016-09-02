import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by alexia on 29/08/2016.
 */
public class LSPattern implements Serializable {

    final static Logger logger = LoggerFactory.getLogger(LSPattern.class);

    public DataFrame loadCSVToDataFrame(SQLContext sqlContext, String fileName) {
      logger.info("Loading CSV file {} into Dataframe", fileName);

        DataFrame dataFrame = sqlContext
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(fileName);

        return dataFrame;
    }

    public JavaPairRDD<Integer, List<String>> findCategoricalPattern (DataFrame categoricalDF) {

        JavaRDD<Row> categoricalRDD = categoricalDF.javaRDD();

        final String[] cols = categoricalDF.columns();

        return categoricalRDD.mapToPair(new PairFunction<Row, Integer, List<String>>() {

            public Tuple2<Integer, List<String>> call(Row t) throws Exception {

                Integer key = t.getInt(0);

                List pattern = new ArrayList();

                for (int i = 1; i <= cols.length -1 ; i++) {
                    if (!t.get(i).equals("")) {
                        String col = cols[i].split("_F")[0];
                        if (!pattern.contains(col)) {
                            pattern.add(col);
                        }
                    }
                }

                Tuple2<Integer, List<String>> tuple = new Tuple2<Integer, List<String>>(key, pattern);
                return tuple;
            }
        });

    }

    public JavaPairRDD<Integer, List<String>> findNumericalPattern (DataFrame numericalDF) {

        JavaRDD<Row> numericalRDD = numericalDF.javaRDD();

        final String[] cols = numericalDF.columns();

        return numericalRDD.mapToPair(new PairFunction<Row, Integer, List<String>>() {

            public Tuple2<Integer, List<String>> call(Row t) throws Exception {

                Integer key = t.getInt(0);

                List pattern = new ArrayList();

                for (int i = 1; i <= cols.length - 2; i++) {
                    if (t.get(i) != null) {
                        String col = cols[i].split("_F")[0];
                        if (!pattern.contains(col)) {
                            pattern.add(col);
                        }
                    }
                }

                Tuple2<Integer, List<String>> tuple = new Tuple2<Integer, List<String>>(key, pattern);
                return tuple;
            }
        });

    }

    public JavaPairRDD mergeCategNum (JavaPairRDD categRDD, JavaPairRDD numRDD) {

        JavaPairRDD mergeRDD = categRDD.join(numRDD);

        return mergeRDD.mapToPair((PairFunction<Tuple2<Integer, Tuple2<List<String>, List<String>>>, String, Integer>) tuple -> {

            Integer id = tuple._1;

            List<String> list1 = tuple._2._1;

            List<String> list2 = tuple._2._2;

            //Firstly remove duplicates:
            list1.removeAll(list2);

            //Then merge two arrayList:
            list1.addAll(list2);

            //Lastly, sort your arrayList if you wish:
            Collections.sort(list1);

            String result = "";

            for (String elt : list1) {
                result = result + elt;
            }

            return new Tuple2<>(result, id);
        });

    }

    public static void main(String[] args) {


        System.out.println("================START LINE STATION PATTERN  ============");

        long startTime = System.currentTimeMillis();

        SparkConf sparkConf = new SparkConf()
                .setAppName("LineStationPattern")
                .set("spark.executor.memory", "16g")
                .setMaster("local[8]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(ctx);

        LSPattern lsPattern = new LSPattern();

        System.out.println("================ LOAD NUMERICAL  ============");

        DataFrame numericalDF = lsPattern.loadCSVToDataFrame(sqlContext,
        "src/main/ressources/numSmall.csv"); //train_numeric.csv

        System.out.println("================ FIND NUMERICAL PATTERN ============");

        JavaPairRDD<Integer, List<String>> numRDD = lsPattern.findNumericalPattern(numericalDF);

        System.out.println("================ LOAD CATEGORICAL  ============");

        DataFrame categoricalDF = lsPattern.loadCSVToDataFrame(sqlContext,
                "src/main/ressources/categSmall.csv"); //"  train_categorical.csv

        System.out.println("================ FIND CATEGORICAL PATTERN ============");

        JavaPairRDD<Integer, List<String>> categRDD = lsPattern.findCategoricalPattern(categoricalDF);

        System.out.println("================ MERGE CATEGORICAL & NUMERICAL PATTERN ============");

        JavaPairRDD<String, Integer> mergeCategNumRDD = lsPattern.mergeCategNum(categRDD, numRDD);

        JavaPairRDD result = mergeCategNumRDD.groupByKey();

       // result.saveAsTextFile("pattern.csv");

        System.out.println(result.collect().size());

        System.out.println("================END LINE STATION PATTERN ============");
    }

    public static void displayMap3(Map<String, List<Integer>> map) {
        for (Map.Entry<String, List<Integer>> entry : map.entrySet())
        {
            System.out.print(entry.getKey() + "/" + entry.getValue());
           /* for (int i = 0 ; i  entry.getValue()) {
                System.out.print(val);
            }*/
            System.out.println();
        }
    }

    public void displayMap(Map<Integer, List<String>> map) {
        for (Map.Entry<Integer, List<String>> entry : map.entrySet())
        {
            System.out.print(entry.getKey() + "/");
            for (String val : entry.getValue()) {
                System.out.print(val);
            }
            System.out.println();
        }
    }

    public static void displayMap2(Map<Integer, String> map) {
        for (Map.Entry<Integer, String> entry : map.entrySet())
        {
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }
    }
}
