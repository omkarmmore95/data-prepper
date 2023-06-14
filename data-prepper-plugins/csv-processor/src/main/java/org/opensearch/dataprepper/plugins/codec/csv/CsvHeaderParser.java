/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class CsvHeaderParser {

    public static List<String> headerParser(String location) throws Exception {
        try {
            List<String> headerList = new ArrayList<>();
            CSVReader reader = new CSVReader(new FileReader(location));
            String[] header = reader.readNext();
            if (header != null) {
                for (String columnName : header) {
                    headerList.add(columnName);
                }
            } else {
                throw new Exception("Header not found in CSV Header file.");
            }
            return headerList;
        } catch (FileNotFoundException e) {
            throw new Exception("CSV Header file not Found");
        }
    }
}
