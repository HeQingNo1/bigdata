package hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Test {
    public static void main(String[] args) {
        YearSubUDF yearSubUDF = new YearSubUDF();
        Timestamp year = yearSubUDF.evaluate(Timestamp.valueOf("2010-01-01 00:00:00"), 3);
        System.out.println(year);

    }

}

