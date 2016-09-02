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

        assertEquals(9,numRDDMap.size());

        boolean found = false;
        for (Map.Entry<Integer, List<String>> entry : numRDDMap.entrySet())
        {
            if (entry.getKey() == 4) {
                assertTrue("L0_S0".equals(entry.getValue().get(0)));
                assertTrue("L3_S51".equals(entry.getValue().get(entry.getValue().size() - 1)));
                found = true;
            }
        }
        assertTrue(found);
    }

    @Test
    public void shouldFindCategoricalPattern() {

        JavaPairRDD<Integer, List<String>> categRDD = lsPattern.findCategoricalPattern(categoricalDF);

        Map<Integer, List<String>> categRDDMap = categRDD.collectAsMap();

        assertEquals(9,categRDDMap.size());

        boolean found = false;
        for (Map.Entry<Integer, List<String>> entry : categRDDMap.entrySet())
        {
            if (entry.getKey() == 4) {
                assertTrue("L0_S1".equals(entry.getValue().get(0)));
                assertTrue("L3_S49".equals(entry.getValue().get(entry.getValue().size() - 1)));
                found = true;
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

        assertEquals(8,patternRDDMap.size());

        boolean found = false;
        Set<Map.Entry> entrySet = patternRDDMap.entrySet();
        for (Map.Entry entry : entrySet)
        {
            if (entry.getKey().equals("L0_S0L0_S1L0_S10L0_S2L0_S23L0_S4L0_S7L0_S8L0_S9L1_S24L1_S25L2_S27L2_S28L3_S29L3_S30L3_S32L3_S33L3_S34L3_S36L3_S37L3_S38L3_S39L3_S40L3_S41L3_S43L3_S44L3_S45L3_S47L3_S48L3_S49L3_S50L3_S51")) {
                Iterator<Integer> it = ((Collection<Integer>)entry.getValue()).iterator();
                assertTrue(it.next() == 18);
                assertTrue(it.next() == 9);
                assertTrue(it.hasNext() == false);
                found = true;
            }
        }
        assertTrue(found);
    }

}
