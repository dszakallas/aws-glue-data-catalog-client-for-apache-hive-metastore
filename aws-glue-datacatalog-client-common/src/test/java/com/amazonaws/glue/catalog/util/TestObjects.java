package com.amazonaws.glue.catalog.util;

import com.amazonaws.glue.catalog.converters.SkewedValueConverter;
import com.amazonaws.services.glue.model.*;
import com.google.common.collect.Maps;

import java.util.*;

public class TestObjects {

    public static Partition getTestPartition(String dbName, String tblName, List<String> values) {
        return new Partition()
                .withDatabaseName(dbName)
                .withTableName(tblName)
                .withValues(values)
                .withCreationTime(new Date(System.currentTimeMillis() / 1000 * 1000))
                .withLastAccessTime(new Date(System.currentTimeMillis() / 1000 * 1000))
                .withParameters(Maps.<String,String>newHashMap())
                .withStorageDescriptor(TestObjects.getTestStorageDescriptor());
    }

    public static StorageDescriptor getTestStorageDescriptor() {
        StorageDescriptor sd = new StorageDescriptor();
        List<String> cols = new ArrayList<>();
        cols.add("sampleCols");
        sd.setBucketColumns(cols);
        sd.setColumns(getTestFieldList());
        sd.setParameters(new HashMap<String, String>());
        sd.setSerdeInfo(getTestSerdeInfo());
        sd.setSkewedInfo(getSkewedInfo());
        sd.setSortColumns(new ArrayList<Order>());
        sd.setInputFormat("inputFormat");
        sd.setOutputFormat("outputFormat");
        sd.setLocation("/test-table");
        sd.withSortColumns(new Order().withColumn("foo").withSortOrder(1));
        sd.setCompressed(false);
        sd.setStoredAsSubDirectories(false);
        sd.setNumberOfBuckets(0);
        return sd;
    }

    public static List<Column> getTestFieldList() {
        List<Column> fieldList = new ArrayList<>();
        Column field = new Column()
                .withComment(UUID.randomUUID().toString())
                .withName("column" + UUID.randomUUID().toString().replaceAll("[^a-zA-Z0-9]+", ""))
                .withType("string");
        fieldList.add(field);
        return fieldList;
    }

    public static SerDeInfo getTestSerdeInfo() {
        return new SerDeInfo()
                .withName("serdeName")
                .withSerializationLibrary("serdeLib")
                .withParameters(new HashMap<String, String>());
    }

    public static SkewedInfo getSkewedInfo() {
        List<String> skewedName = new ArrayList<>();
        List<String> skewedValue = new ArrayList<>();
        List<String> skewedMapKey = new ArrayList<>();
        List<List<String>> skewedValueList = new ArrayList<>();
        skewedName.add(UUID.randomUUID().toString());
        skewedName.add(UUID.randomUUID().toString());
        skewedValue.add(UUID.randomUUID().toString());
        skewedValue.add(UUID.randomUUID().toString());
        skewedValueList.add(skewedValue);
        skewedMapKey.add(UUID.randomUUID().toString());
        skewedMapKey.add(UUID.randomUUID().toString());
        Map<String, String> skewedMap = new HashMap<>();
        skewedMap.put(SkewedValueConverter.convertListToString(skewedMapKey), UUID.randomUUID().toString());

        return new SkewedInfo().withSkewedColumnValueLocationMaps(skewedMap).withSkewedColumnNames(skewedName)
                .withSkewedColumnValues(SkewedValueConverter.convertSkewedValue(skewedValueList));
    }

    public static PartitionError getPartitionError(List<String> values, Exception exception) {
        return new PartitionError()
                .withPartitionValues(values)
                .withErrorDetail(new ErrorDetail()
                        .withErrorCode(exception.getClass().getSimpleName())
                        .withErrorMessage(exception.getMessage()));
    }
}
