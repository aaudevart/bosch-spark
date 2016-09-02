import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Created by alexia on 29/08/2016.
 */
public class LSPatternTest {

    private  static SQLContext sqlContext;

    private LSPattern lsPattern;

    private DataFrame categoricalDF;

    private DataFrame numericalDF;


    @BeforeClass
    public static void setup() {

       SparkConf sparkConf = new SparkConf()
                .setAppName("LineStationPattern")
                .set("spark.executor.memory", "16g")
                .setMaster("local[8]");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        sqlContext = new SQLContext(ctx);

    }


    @Before
    public  void setupBeforeEachTest() {

        lsPattern = new LSPattern();

        categoricalDF = lsPattern.loadCSVToDataFrame(sqlContext,
                "data/categSmall.csv");

        numericalDF = lsPattern.loadCSVToDataFrame(sqlContext,
                "data/numSmall.csv");
    }

    @Test
    public void shouldFindNumericalPattern() {

        JavaPairRDD<Integer, List<String>> numRDD = lsPattern.findNumericalPattern(numericalDF);
        Map<Integer, List<String>> numRDDMap = numRDD.collectAsMap();

        assertEquals(5,numRDDMap.size());

        for (Map.Entry<Integer, List<String>> entry : numRDDMap.entrySet())
        {
            if (entry.getKey() == 4 || entry.getKey() == 9 || entry.getKey() == 18) {
                assertTrue("L0_S0".equals(entry.getValue().get(0)));
                assertTrue("L0_S2".equals(entry.getValue().get(1)));
                assertTrue("L0_S4".equals(entry.getValue().get(2)));
                assertTrue("L0_S7".equals(entry.getValue().get(3)));
            }

            if (entry.getKey() == 6) {
                assertTrue(entry.getValue().isEmpty());
            }

            if (entry.getKey() == 7) {
                assertTrue("L0_S0".equals(entry.getValue().get(0)));
                assertTrue("L0_S2".equals(entry.getValue().get(1)));
                assertTrue("L0_S5".equals(entry.getValue().get(2)));
                assertTrue("L0_S6".equals(entry.getValue().get(3)));
            }
        }
        assertEquals(numRDDMap.entrySet().size(), 5);
    }

    @Test
    public void shouldFindCategoricalPattern() {

        JavaPairRDD<Integer, List<String>> categRDD = lsPattern.findCategoricalPattern(categoricalDF);

        Map<Integer, List<String>> categRDDMap = categRDD.collectAsMap();

        assertEquals(5,categRDDMap.size());

        boolean found = true;
        for (Map.Entry<Integer, List<String>> entry : categRDDMap.entrySet())
        {
            if (entry.getKey() == 4 || entry.getKey() == 9 ) {
                assertTrue("L0_S0".equals(entry.getValue().get(0)));
                assertTrue("L0_S2".equals(entry.getValue().get(1)));
                assertTrue("L0_S3".equals(entry.getValue().get(2)));
                assertTrue("L3_S29".equals(entry.getValue().get(3)));
            } else if (entry.getKey() == 18) {
                assertTrue("L0_S2".equals(entry.getValue().get(0)));
                assertTrue("L0_S3".equals(entry.getValue().get(1)));
                assertTrue("L3_S29".equals(entry.getValue().get(2)));
            }else if (entry.getKey() == 6) {
                assertTrue("L0_S3".equals(entry.getValue().get(0)));

            } else if (entry.getKey() == 7) {
                assertTrue("L0_S2".equals(entry.getValue().get(0)));
                assertTrue("L0_S3".equals(entry.getValue().get(1)));
                assertTrue("L3_S29".equals(entry.getValue().get(2)));
            } else {
                found = false;
            }
        }
        assertTrue(found);
    }


    @Test
    public void shouldMergeCategNum() {

        JavaPairRDD<Integer, List<String>> categRDD = lsPattern.findCategoricalPattern(categoricalDF);
        JavaPairRDD<Integer, List<String>> numRDD = lsPattern.findNumericalPattern(numericalDF);
        JavaPairRDD patternRDD = lsPattern.mergeCategNum(categRDD, numRDD);

        Map patternRDDMap = patternRDD.collectAsMap();

        assertEquals(3,patternRDDMap.size());

        boolean found = true;
        Set<Map.Entry> entrySet = patternRDDMap.entrySet();
        for (Map.Entry entry : entrySet)
        {
            if (entry.getKey().equals("L0_S3")) {
                Iterator<Integer> it = ((Collection<Integer>)entry.getValue()).iterator();
                assertTrue(it.next() == 6);
                assertTrue(it.hasNext() == false);
            } else if (entry.getKey().equals("L0_S0L0_S2L0_S3L0_S4L0_S7L3_S29")) {
                Iterator<Integer> it = ((Collection<Integer>)entry.getValue()).iterator();
                assertTrue(it.next() == 4);
                assertTrue(it.next() == 18);
                assertTrue(it.next() == 9);
                assertTrue(it.hasNext() == false);
            } else if (entry.getKey().equals("L0_S0L0_S2L0_S3L0_S5L0_S6L3_S29")) {
                Iterator<Integer> it = ((Collection<Integer>)entry.getValue()).iterator();
                assertTrue(it.next() == 7);
                assertTrue(it.hasNext() == false);
            } else {
                found = false;
            }
        }
        assertTrue(found);
    }

}
