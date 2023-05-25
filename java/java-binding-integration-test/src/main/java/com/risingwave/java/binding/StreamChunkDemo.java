// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.java.binding;

import java.io.IOException;

public class StreamChunkDemo {
    public static double getValue(StreamChunkRow rowData) {
        int value1 = (int) rowData.getShort(0);
        int value2 = (int) rowData.getInt(1);
        Long value3 = (Long) rowData.getLong(2);
        float value4 = (float) rowData.getFloat(3);
        double value5 = (double) rowData.getDouble(4);
        Boolean value6 = (Boolean) rowData.getBoolean(5);
        String value7 = (String) rowData.getString(6);
        java.sql.Timestamp value8 = (java.sql.Timestamp) rowData.getTimestamp(7);
        int value9 = rowData.getDecimal(8).intValue();
        boolean mayNull = rowData.isNull(9);
        return value1 + value2 + value3 + value4 + value5 + value9;
    }

    public static void main(String[] args) throws IOException {
        byte[] payload = System.in.readAllBytes();
        for (int t = 0; t < 10; t++) {
            StreamChunkIterator iter = new StreamChunkIterator(payload);
            long startTime = System.currentTimeMillis();
            while (true) {
                try (StreamChunkRow row = iter.next()) {
                    if (row == null) {
                        break;
                    }
                    getValue(row);
                }
            }
            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime;
            System.out.println("Time elapsed: " + elapsedTime + " milliseconds");
        }
    }
}
