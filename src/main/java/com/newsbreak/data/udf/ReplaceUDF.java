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

/**
 * ReplaceUDF
 * @author will
 */

@Description(name = "ReplaceUDF", value = "ReplaceUDF(string_expression, string_pattern , string_replacement) returns an new string",
 extended = "ReplaceUDF(string_expression, string_pattern , string_replacement) returns an new string")
public class ReplaceUDF extends GenericUDF {
  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 3) {
      throw new UDFArgumentLengthException(
          "ReplaceUDF(string_expression, string_pattern , string_replacement) takes exactly 3 arguments.");
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
    assert (arguments.length == 3);

    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    Text sourceText = (Text) converters[0].convert(arguments[0].get());
    Text patternText = (Text) converters[1].convert(arguments[1].get());
    Text replacementText = (Text) converters[2].convert(arguments[2].get());

    return sourceText.toString().replace(patternText.toString(), replacementText.toString());
  }
  
  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 3);
    return "ReplaceUDF(" + children[0] + ", " + children[1] + ", " + children[2] + ")";
  }

}