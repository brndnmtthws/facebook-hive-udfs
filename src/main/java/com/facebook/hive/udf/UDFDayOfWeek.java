package com.facebook.hive.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Return the integer corresponding to day-of-week for a given datestamp.
 * 0 is Sunday. NULL returns NULL.
 *
 */
@Description(name = "udfdayofweek",
             value = "_FUNC_(year, month, day) - Find the weekday of the date. Use four-day year (2011); January is month=1, January 1 is day=1. Also takes an array representing these.\n")
public class UDFDayOfWeek extends UDF {
    public Integer evaluate(Integer year, Integer month, Integer day) throws UDFArgumentException {
        if (year == null || month == null || month < 1 || month > 12 || day == null || day < 1 || day > 31) {
          throw new UDFArgumentException("Error: Year-Month-Day outside of valid range!\n");
        }
        // January = 1 in Hive, so fix month here.
        Calendar date = new GregorianCalendar(year, month-1, day);
        // Sunday = 0 in the rest of the world, so fix day here.
	return date.get(Calendar.DAY_OF_WEEK) - 1;
    }
    public Integer evaluate(ArrayList<String> datearr) throws UDFArgumentException {
        if (datearr.size() != 3) {
          throw new UDFArgumentException("Must provide a size-3 array, containing Year, Month, and Day in order.");
        }
        Integer year = Integer.parseInt(datearr.get(0));
        Integer month = Integer.parseInt(datearr.get(1));
        Integer day = Integer.parseInt(datearr.get(2));
        if (year == null || month == null || month < 1 || month > 12 || day == null || day < 1 || day > 31) {
          throw new UDFArgumentException("Error: Year-Month-Day outside of valid range!\n");
        }
        // January = 1 in Hive, so fix month here.
        Calendar date = new GregorianCalendar(year, month-1, day);
        // Sunday = 0 in the rest of the world, so fix day here.
	return date.get(Calendar.DAY_OF_WEEK) - 1;
    }
}
