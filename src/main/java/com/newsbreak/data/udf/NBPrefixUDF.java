package com.newsbreak.data.udf;

import org.json.JSONObject;
import java.util.Iterator;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.json.JSONException;

/**
 * NBPrefix UDF
 * @author will
 */

@Description(name = "NBPrefix", value = "NBPrefix(json, path) returns an array cotaining objects in JSON, specified by PATH",
 extended = "NBPrefix(json, path) returns an array cotaining objects in JSON, specified by PATH")
public class NBPrefixUDF extends GenericUDF {
  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The function NBPrefix(s, path_to_array) takes exactly 2 arguments.");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException, JSONException {
    assert (arguments.length == 2);

    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    Text jsonText = (Text) converters[0].convert(arguments[0].get());
    Text prefixText = (Text) converters[1].convert(arguments[1].get());
    String prefix = prefixText.toString();

    JSONObject fromJsonObject = new JSONObject(jsonText.toString());

    JSONObject toJsonObject = new JSONObject();
    traverse(fromJsonObject, toJsonObject, prefix);

    return toJsonObject.toString();
  }

  /** Performs a DFS traversal on the JSON object/array specified
  *   by X, and adding objects specified by NAMES to arraylist RES
  */
  private static void traverse(JSONObject fromJsonObject, JSONObject toJsonObject, String prefix) {

    Iterator<String> keys = fromJsonObject.keys();
    while(keys.hasNext()) {
      String key = keys.next();
      if (key.startsWith(prefix)) {
        Object value = fromJsonObject.get(key);
        if (value instanceof String) {
          toJsonObject.put(key.substring(prefix.length()), value);
        }
      }
    }

  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "NBPrefixUDF(" + children[0] + ", " + children[1] + ")";
  }

}