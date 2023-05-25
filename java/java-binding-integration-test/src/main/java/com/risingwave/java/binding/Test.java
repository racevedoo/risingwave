package com.risingwave.java.binding;

import java.util.ArrayList;

public class Test {
    static int loopTime = 500000;

    public static ArrayList<Object> myFunction(int index) {
        short v1 = (short) index;
        int v2 = (int) index;
        long v3 = (long) index;
        float v4 = (float) index;
        double v5 = (double) index;
        boolean v6 = index % 3 == 0;
        String v7 =
                "'"
                        + new String(new char[(index % 10) + 1])
                                .replace("\0", String.valueOf(index))
                        + "'";
        String v8 = "to_timestamp(" + index + ")";
        int v9 = index;
        // Integer mayNull = (index % 5 == 0) ? null : (int) index;
        Integer mayNull = null;

        ArrayList<Object> rowData = new ArrayList<>();
        rowData.add(v1);
        rowData.add(v2);
        rowData.add(v3);
        rowData.add(v4);
        rowData.add(v5);
        rowData.add(v6);
        rowData.add(v7);
        rowData.add(v8);
        rowData.add(v9);
        rowData.add(mayNull);

        // System.out.println(value1);
        // System.out.println(value2);
        // System.out.println(value3);
        // for (Object item : rowData) {
        // System.out.println(item);
        // }
        return rowData;
    }

    public static double processRowData(ArrayList<Object> rowData) {
        // for (int i = 0; i < loopTime; i++) {
        short value1 = (short) rowData.get(0);
        int value2 = (int) rowData.get(1);
        long value3 = (long) rowData.get(2);
        float value4 = (float) rowData.get(3);
        double value5 = (double) rowData.get(4);
        boolean value6 = (boolean) rowData.get(5);
        //        String value7 = (String) rowData.get(6);
        //        String value8 = (String) rowData.get(7);
        //        int value9 = (int) rowData.get(8);
        Integer mayNull = (Integer) rowData.get(9);
        return value1 + value2 + value3 + value4 + value5;
        // }
        // System.out.println(value1);
        // System.out.println(value2);
        // System.out.println(value3);
        // System.out.println(value4);
        // System.out.println(value5);
        // System.out.println(value6);
        // System.out.println(value7);
        // System.out.println(value8);
        // System.out.println(value9);
        // System.out.println(mayNull);
    }

    public static void main(String[] args) {
        // Start measuring the time
        ArrayList<ArrayList<Object>> data = new ArrayList<>();
        for (int i = 0; i < loopTime; i++) {
            data.add(myFunction(i));
        }

        for (int t = 0; t < 10; t++) {
            long startTime = System.currentTimeMillis();
            // Call your function here
            for (int i = 0; i < loopTime; i++) {
                processRowData(data.get(i));
            }

            // Stop measuring the time
            long endTime = System.currentTimeMillis();

            // Calculate the time elapsed
            long elapsedTime = endTime - startTime;

            // Print the elapsed time
            System.out.println("Time elapsed: " + elapsedTime + " milliseconds");
        }
    }
}
