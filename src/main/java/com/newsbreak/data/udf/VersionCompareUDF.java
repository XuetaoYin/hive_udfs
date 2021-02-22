package com.newsbreak.data.udf;

import com.newsbreak.data.utils.VersionUtils;
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
 * VersionCompareUDF
 * @author will
 */

@Description(name = "VersionCompareUDF", value = "VersionCompareUDF(string_left, string_right) returns int, 1 means left greater than right, 0 means equal, -1 means left less than right",
 extended = "VersionCompareUDF(string_left, string_right) returns int, 1 means left greater than right, 0 means equal, -1 means left less than right")
public class VersionCompareUDF extends GenericUDF {
  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "VersionCompareUDF(string_left, string_right) takes exactly 2 arguments.");
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



    try {
      Text textLeft = (Text) converters[0].convert(arguments[0].get());
      Text textRight = (Text) converters[1].convert(arguments[1].get());
      return VersionUtils.compareVersion(textLeft.toString(), textRight.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
  
  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "VersionCompareUDF(" + children[0] + ", " + children[1] + ")";
  }

}