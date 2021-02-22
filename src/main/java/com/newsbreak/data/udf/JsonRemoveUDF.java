package com.newsbreak.data.udf;

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
import org.json.JSONObject;

/**
 * JsonRemoveUDF
 * @author will
 */

@Description(name = "JsonRemoveUDF", value = "JsonRemoveUDF(json_str, key) returns an new string",
 extended = "JsonRemoveUDF(json_str, key) returns an new string")
public class JsonRemoveUDF extends GenericUDF {
  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "JsonRemoveUDF(json_str, key) takes exactly 2 arguments.");
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

    JSONObject fromJsonObject = new JSONObject(jsonText.toString());
    fromJsonObject.remove(prefixText.toString());
    return fromJsonObject.toString();

  }
  
  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "JsonRemoveUDF(" + children[0] + ", " + children[1] + ")";
  }

}