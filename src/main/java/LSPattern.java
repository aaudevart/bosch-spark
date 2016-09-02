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

    private Map<String, String> buildHeaderLineCateg(List<String> cols, Row values) {

        Map<String, String> res = new HashMap<>();

        for (int i = 1; i < cols.size(); i++) {
            res.put(cols.get(i), values.getString(i));
        }

        return res;
    }

    private Map<String, Double> buildHeaderLineNum(List<String> cols, Row values) {

        Map<String, Double> res = new HashMap<>();

        for (int i = 1; i < cols.size() - 1; i++) {
            Double val = null;
            if (values.get(i) instanceof  Double) {
                val = (Double) values.get(i);
            }
            res.put(cols.get(i), val);
        }

        return res;
    }

    public JavaPairRDD<Integer, List<String>> findCategoricalPattern(DataFrame categoricalDF) {

        JavaRDD<Row> categoricalRDD = categoricalDF.javaRDD();

        List<String> columns = Arrays.asList(categoricalDF.columns());

        return categoricalRDD.mapToPair((PairFunction<Row, Integer, List<String>>) row -> {

            Integer key = row.getInt(0);

            Map<String, String> headerLinesMap = buildHeaderLineCateg(columns, row);

            Set<String> pattern = new HashSet<>();

            headerLinesMap.entrySet()
                    .stream()
                    .filter(s -> !s.getValue().isEmpty()) // nom de la colonne et non la valeur !
                    .forEach(s -> {
                        String elt = s.getKey().split("_F")[0];
                        pattern.add(elt);
                    });

            List result = new ArrayList<>(pattern);
            Collections.sort(result);

            return new Tuple2<>(key, result);
        });

    }

    public JavaPairRDD<Integer, List<String>> findNumericalPattern(DataFrame numericalDF) {

        JavaRDD<Row> numericalRDD = numericalDF.javaRDD();

        List<String> columns = Arrays.asList(numericalDF.columns());

        return numericalRDD.mapToPair((PairFunction<Row, Integer, List<String>>) row -> {

            Integer key = row.getInt(0);

            Map<String, Double> headerLinesMap = buildHeaderLineNum(columns, row);

            Set<String> pattern = new HashSet<>();

            headerLinesMap.entrySet()
                    .stream()
                    .filter(rowValue -> rowValue.getValue() != null)
                    .forEach(column -> {
                        String col = column.getKey().split("_F")[0];
                        pattern.add(col);
                    });

            List result = new ArrayList<>(pattern);
            Collections.sort(result);

            return new Tuple2<>(key, result);
        });

    }

    public JavaPairRDD<String, List<Integer>> mergeCategNum(JavaPairRDD categRDD, JavaPairRDD numRDD) {

        JavaPairRDD mergeRDD = categRDD.join(numRDD);

        JavaPairRDD mergeFlatRDD = mergeRDD.mapToPair((PairFunction<Tuple2<Integer, Tuple2<List<String>, List<String>>>, String, Integer>) tuple -> {

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

        return mergeFlatRDD.groupByKey();

    }

    public static void main(String[] args) {

        System.out.println("================START LINE STATION PATTERN  ============");

        SparkConf sparkConf = new SparkConf()
                .setAppName("LineStationPattern")
                .set("spark.executor.memory", "16g")
                .setMaster("local[8]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(ctx);

        LSPattern lsPattern = new LSPattern();

        System.out.println("================ LOAD NUMERICAL  ============");

        DataFrame numericalDF = lsPattern.loadCSVToDataFrame(sqlContext,"file:///Users/alexia/Documents/Projets/02-perso/Kaggle/bosch/data/train_numeric.csv");

              //  "data/numSmall.csv"); //train_numeric.csv

        System.out.println("================ FIND NUMERICAL PATTERN ============");

        JavaPairRDD<Integer, List<String>> numRDD = lsPattern.findNumericalPattern(numericalDF);

        System.out.println("================ LOAD CATEGORICAL  ============");

        DataFrame categoricalDF = lsPattern.loadCSVToDataFrame(sqlContext,"file:///Users/alexia/Documents/Projets/02-perso/Kaggle/bosch/data/train_categorical.csv");
               // "data/categSmall.csv"); //"  train_categorical.csv

        System.out.println("================ FIND CATEGORICAL PATTERN ============");

        JavaPairRDD<Integer, List<String>> categRDD = lsPattern.findCategoricalPattern(categoricalDF);

        System.out.println("================ MERGE CATEGORICAL & NUMERICAL PATTERN ============");

        JavaPairRDD result = lsPattern.mergeCategNum(categRDD, numRDD);

        result.repartition(1).saveAsTextFile("file:///Users/alexia/Documents/Projets/02-perso/Kaggle/bosch/bosch-spark/pattern.csv");

        System.out.println(result.collect().size());

        System.out.println("================END LINE STATION PATTERN ============");
    }
}
