package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class YearSubUDF extends UDF {
    public Timestamp evaluate(Timestamp datetime, int num){
        /**
         * DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         *         Calendar calendar = Calendar.getInstance();
         *         Date begin;
         *         String result = null;
         *         try {
         *             begin= sdf.parse(datetime);
         *             calendar.setTime(begin);
         *             calendar.add(Calendar.YEAR, -num);
         *             result = sdf.format(calendar.getTime());
         *
         *         } catch (Exception e) {
         *             e.printStackTrace();
         *         }
         *         return Timestamp.valueOf(result);
         */
        DateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar=Calendar.getInstance();
        String result = null;
        try {
            calendar.setTime(datetime);
            calendar.add(Calendar.YEAR,-num);
            result=sdf.format(calendar.getTime());

        } catch (Exception e) {
            e.printStackTrace();
        }
        return Timestamp.valueOf(result);
    }

}