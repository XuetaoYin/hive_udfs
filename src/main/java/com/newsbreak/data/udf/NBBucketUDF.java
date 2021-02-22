package com.newsbreak.data.udf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * NBBucket UDF
 * @author will
 */

@Description(name = "NBBucket", value = "NBBucket(json, path) returns an array cotaining objects in JSON, specified by PATH",
 extended = "NBBucket(json, path) returns an array cotaining objects in JSON, specified by PATH")
public class NBBucketUDF extends GenericUDF {
  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The function NBBucket(s, path_to_array) takes exactly 2 arguments.");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
            .writableStringObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException, JSONException {
    assert (arguments.length == 2);

    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    Text s = (Text) converters[0].convert(arguments[0].get());
    Text resource = (Text) converters[1].convert(arguments[1].get());

    ArrayList<Text> results = new ArrayList<Text>();
    JSONObject obj = new JSONObject(s.toString());
    String[] names = resource.toString().split("\\.");

    traverse(results, obj, names, 0);

    HashSet h = new HashSet(results);
    results.clear();
    results.addAll(h);

    return results;
  }

  /** Performs a DFS traversal on the JSON object/array specified
  *   by X, and adding objects specified by NAMES to arraylist RES
  */
  private static void traverse(ArrayList<Text> res, Object x, String[] names, int index) {
    JSONObject jsonObject = (JSONObject) x;
    Iterator<String> keys = jsonObject.keys();
    while(keys.hasNext()) {
      String key = keys.next();

      if (key.equals("buckets")) {
        Object value = jsonObject.get(key);
        if (value instanceof JSONArray) {
          JSONArray arr = (JSONArray) value;
          for (int i = 0; i < arr.length(); i += 1) {
            res.add(new Text(arr.get(i).toString()));
          }
        }
      }

      if (key.startsWith("bucket-")) {
        Object value = jsonObject.get(key);
        if (value instanceof String) {
          res.add(new Text(key.substring("bucket-".length()) + "-" + value));
        }
      }
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "NBBucketUDF(" + children[0] + ", " + children[1] + ")";
  }

}