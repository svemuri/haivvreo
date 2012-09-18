/*
 * Copyright 2011 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.haivvreo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.Writable;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.io.IOException;

import static com.linkedin.haivvreo.AvroSerDe.HAIVVREO_SCHEMA;
import static com.linkedin.haivvreo.HaivvreoUtils.SCHEMA_LITERAL;
import static org.junit.Assert.*;

public class TestColSelectiveSerde {
  private final GenericData GENERIC_DATA = GenericData.get();

  public static final String LOCALRECORD_SCHEMA = "{\n" +
      "  \"namespace\": \"testing.test.mctesty\",\n" +
      "  \"name\": \"oneRecord\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"string1\",\n" +
      "      \"type\":\"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"string2\",\n" +
      "      \"type\":\"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"int1\",\n" +
      "      \"type\":\"int\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"aRecord\",\n" +
      "      \"type\":{\"type\":\"record\",\n" +
      "              \"name\":\"recordWithinARecord\",\n" +
      "              \"fields\": [\n" +
      "                 {\n" +
      "                  \"name\":\"int1\",\n" +
      "                  \"type\":\"int\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"boolean1\",\n" +
      "                  \"type\":\"boolean\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"long1\",\n" +
      "                  \"type\":\"long\"\n" +
      "                }\n" +
      "      ]}\n" +
      "    }\n" +
      "  ]\n" +
      "}";

   


 @Test
  public void canDeserializeColumnsSelectively() throws SerDeException, IOException {
    Schema s = Schema.parse(LOCALRECORD_SCHEMA);
    GenericData.Record record = new GenericData.Record(s);

    record.put("string1", "Wandered Aimlessly");
    record.put("string2", "as a cloud");
    record.put("int1", 555);

    GenericData.Record innerRecord = new GenericData.Record(s.getField("aRecord").schema());
    innerRecord.put("int1", 42);
    innerRecord.put("boolean1", true);
    innerRecord.put("long1", 42432234234l);
    record.put("aRecord", innerRecord);

    assertTrue(GENERIC_DATA.validate(s, record));

    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(s);

    AvroSerDe des = new AvroSerDe();
    Configuration hconf = new Configuration();
    Properties tblProps = new Properties();
    tblProps.put(HaivvreoUtils.SCHEMA_LITERAL, LOCALRECORD_SCHEMA);

    ArrayList<Integer> queryCols = new ArrayList<Integer>();
    queryCols.add(3);
    queryCols.add(1);
    queryCols.add(1);
    queryCols.add(3);
    queryCols.add(1);
    ColumnProjectionUtils.appendReadColumnIDs(hconf, queryCols);
    
    
    // Initialize deserializer
    des.initialize(hconf, tblProps);
    
    ArrayList<Object> row = (ArrayList<Object>)des.deserialize(garw);
    assertEquals(4, row.size());

    Object stringObj = row.get(1);
    assertEquals(stringObj, "as a cloud");

    Object theRecordObject = row.get(3);
    System.out.println("theRecordObject = " + theRecordObject.getClass().getCanonicalName());

    Object theIntObject = row.get(2);
    assertEquals(theIntObject, null);

    // The original record was lost in the deserialization, so just go the correct way, through objectinspectors
    StandardStructObjectInspector oi = (StandardStructObjectInspector)aoig.getObjectInspector();
    List<? extends StructField> allStructFieldRefs = oi.getAllStructFieldRefs();
    assertEquals(4, allStructFieldRefs.size());
    StructField fieldRefForaRecord = allStructFieldRefs.get(3);
    assertEquals("arecord", fieldRefForaRecord.getFieldName());
    Object innerRecord2 = oi.getStructFieldData(row, fieldRefForaRecord); // <--- use this!

    // Extract innerRecord field refs
    StandardStructObjectInspector innerRecord2OI = (StandardStructObjectInspector) fieldRefForaRecord.getFieldObjectInspector();

    List<? extends StructField> allStructFieldRefs1 = innerRecord2OI.getAllStructFieldRefs();
    assertEquals(3, allStructFieldRefs1.size());
    assertEquals("int1", allStructFieldRefs1.get(0).getFieldName());
    assertEquals("boolean1", allStructFieldRefs1.get(1).getFieldName());
    assertEquals("long1", allStructFieldRefs1.get(2).getFieldName());

    innerRecord2OI.getStructFieldsDataAsList(innerRecord2);
    assertEquals(42, innerRecord2OI.getStructFieldData(innerRecord2, allStructFieldRefs1.get(0)));
    assertEquals(true, innerRecord2OI.getStructFieldData(innerRecord2, allStructFieldRefs1.get(1)));
    assertEquals(42432234234l, innerRecord2OI.getStructFieldData(innerRecord2, allStructFieldRefs1.get(2)));
  }
}
