package brickhouse.udf.json;
/**
 *  Generate a Hive structure from a JSON string.
 *  To define the schema of the structure, one object is
 *   passed in as an example, and objects of that return type will be returned.
 *   This example object must be a constant value. For maps with mixed types, one can pass in
 *    a named_struct, with the field names matching onto the map keys.
 *
 *  i.e. for a JSON string like
 *    ' {"actor":"jerome", "score",57.0, "actionList": [ "LIKE","COMMENT" ] ,
 *         "actorScoreMap": { "bob":23, "mao":345 } } '
 *
 *  one could parse it with
 *    from_json( json_str, named_struct("actor", "", "score", 0.0, "actionList", array(""),
 *                         "actorScoreMap", map( "", 0.0 ) )
 *
 *
 */

import com.sun.org.apache.xerces.internal.xs.datatypes.ObjectList;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *  Generate a Map of values from a JSON string
 *
 */
@Description(name="from_json",
    value = "_FUNC_(a,b) - Returns a Map of values from a JSON string"
)
public class FromJsonUDF extends GenericUDF {

   private StandardStructObjectInspector structInspector;
   private StringObjectInspector stringInspector;
   private ObjectMapper om = new ObjectMapper();

   Object[] ret;
   private static final Logger LOG = Logger.getLogger(FromJsonUDF.class);

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    String jsonString =  this.stringInspector.getPrimitiveJavaObject(args[0].get());
    JsonNode node = null;
    try {
      node = om.readTree(jsonString);
    } catch (IOException e) {
      throw new HiveException("Json Parsing Problem : " + jsonString);
    }
    ret = form_named_struct(node, structInspector);
    return ret;
  }

  private List<Object> form_list(JsonNode node, ListObjectInspector loi) throws HiveException{
    ObjectInspector oi = loi.getListElementObjectInspector();
    Category cat = oi.getCategory();
    List<Object> result = new ArrayList<Object>();

    if(!node.isArray())
    {
      throw new HiveException(" expecting array, while " + node.toString() +" is not an array!");
    }
    Iterator<JsonNode> elements = node.getElements();

    if(oi instanceof WritableConstantStringObjectInspector)
    {
      while(elements.hasNext())
      {
        JsonNode elemNode = elements.next();
        if(elemNode.isTextual())
        {
          result.add(elemNode.getTextValue());
        }
        else
        {
          throw new HiveException(" expecting string type, while " + elemNode.toString() + " is not a string");
        }
      }
    }
    else if (cat == Category.STRUCT)
    {
       while(elements.hasNext())
       {
         JsonNode elemNode = elements.next();
         result.add(form_named_struct(elemNode, (StandardStructObjectInspector) oi));
       }
    }

    return result;
  }

  private Object[] form_named_struct(JsonNode node, StandardStructObjectInspector ssoi) throws HiveException {
    List<StructField> structFieldList = (List<StructField>) ssoi.getAllStructFieldRefs();
    Iterator<StructField> iterStructField = structFieldList.iterator();

    Object[] result = new Object[structFieldList.size()];

    int index = 0;

    while(iterStructField.hasNext())
    {
      StructField sf = iterStructField.next();
      String fieldName = sf.getFieldName();
      JsonNode jsNode = node.path(fieldName);
      if(jsNode.isMissingNode())
        throw new HiveException("No matching field name for " + fieldName);

      ObjectInspector oi = sf.getFieldObjectInspector();
      Category cat = oi.getCategory();
      if( oi instanceof WritableConstantStringObjectInspector)
      {
        result[index] = jsNode.getValueAsText();
      }
      else if (oi instanceof WritableConstantBooleanObjectInspector)
      {
        result[index] = jsNode.getBooleanValue();
      }
      else if (oi instanceof WritableConstantDoubleObjectInspector)
      {
        result[index] = jsNode.getDoubleValue();
      }
      else if (oi instanceof WritableConstantLongObjectInspector)
      {
        result[index] = jsNode.getLongValue();
      }
      else if (cat == Category..LIST )
      {
        result[index] = form_list(jsNode, (ListObjectInspector)oi);
      }

      index++;
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] args) {
    // to get a better idea of this

    return "from_json(" + args[0] + ")";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args)
      throws UDFArgumentException {
    if(args.length != 2 ) {
      throw new UDFArgumentException(" from_json takes only takes two arguments (json_string, named_struct)");
    }
    if(args[0] instanceof WritableConstantStringObjectInspector)
    {
      throw new UDFArgumentException(" the first parameter of from_json should be string");
    }
    if(args[1].getCategory() != Category.STRUCT)
    {
      throw new UDFArgumentException(" the second parameter of from_json should be named_struct");
    }

    stringInspector = (StringObjectInspector) args[0];
    structInspector = (StandardStructObjectInspector)args[1];

    List<StructField> structElements = (List<StructField>) structInspector.getAllStructFieldRefs();
    ret = new Object[structElements.size()];

    return structInspector;
  }

}
