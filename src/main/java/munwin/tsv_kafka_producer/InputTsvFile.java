package munwin.tsv_kafka_producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 * Class InputTsvFile
 *
 * This class takes a path to a tab separated value (TSV) file
 * that contains information about messages that should be fed
 * into the sorter, and a batch size that indicates how many
 * individual messages to batch together into a single activeMQ
 * message payload.
 *
 * The TSV file must be in a specified format:
 *
 * The first row should contain a header that names the fields
 * in the message, and the datatype for that field.  The name
 * and the datatype should be written as:
 * name=datatype
 *
 * The values for datatype can be:
 * string
 * integer
 * long
 * double
 * json [pre-formatted json]
 * date(dateformat)
 * array(datatype)
 * map(datatype,datatype)
 *
 * In the case of the date field, the dateformat should be the
 * format of the date in SimpleDateFormat syntax. All dates in
 * that column must exactly match the format specified or the
 * file won't parse.
 *
 * For arrays, the datatype must be one of: string, integer, long, double, json, or date(dateformat)
 * For maps, the datatypes must be one of: string, integer, long, double, json, or date(dateformat)
 * However, note that any values given for the key in a key value pair will be converted to
 * strings when generating the map. Dates will first be converted to a long when used as key.
 *
 * Arrays of arrays, or maps of maps, or arrays of maps, etc, are not allowed.
 *
 * Each row after the header row should contain data, with no blank rows.
 *
 * Arrays should be represented as multiple columns with the same name and datatype.
 * Maps should be represented as multiple columns with the same name and datatype and each
 * field should have the form: key,value (Keys cannot contain commas)
 *
 * json fields should include pre-formated json strings, surrounded by a starting and ending
 * quotation mark, with NO tab characters anywhere in the string.  The name of the field is defined
 * in the header.  For example, if you wanted to have:
 * "name1":{"key1":"value1"} appear in the final message sent to the queue, you would put:
 * name1=json in the header, and then each row would have something like:
 * "{"key1":"value1"}"
 *
 * Example:
 *
 * Field1=string\tField2=long\tField3=date(yyyy-mm-dd hh:mm:ss)\tField4=array(string)\tField4=array(string)\tField4=array(string)\tField5=map(string,integer)\tField6=map(string,integer)\tField6=json
 * value1\t87349874987897\t"2012-12-31 23:59:59"\tarrayValue\tarrayValue2\tarrayValue3\tmapKey1=24\tmapKey2=595\tjsonFieldName="[{"key1":"value1","key2":"value2"},{"key3":"value3","key4":"value4"}]"
 * value1\t98080934850983\t"2012-01-01 00:00:00"\tarrayValue1\tarrayValue2\tarrayValue3\tmapKey1=3454,\tmapKey2=-4\tjsonFieldName="[{"key1":"value1","key2":"value2"},{"key3":"value3","key4":"value4"}]"
 *
 * @author munwin
 *
 */
public class InputTsvFile {
  private final Hashtable<String, DataField> fieldNames = new Hashtable<String, DataField>();
  private final Hashtable<Integer, DataField> columns = new Hashtable<Integer, DataField>();
  private final List<String> messages = new ArrayList<String>();
  private final List<String> batchMessages = new ArrayList<String>();

  @SuppressWarnings("serial")
  private static final Set<String> FIELD_TYPES = new HashSet<String>() {{
    add("string");
    add("integer");
    add("long");
    add("double");
    add("date");
    add("array");
    add("map");
    add("json");
  }};

  @SuppressWarnings("serial")
  private static final Set<String> PRIMITIVE_TYPES = new HashSet<String>() {{
    add("string");
    add("integer");
    add("long");
    add("double");
    add("date");
    add("json");
  }};

  /**
   * InputTsvFile Constructor
   * @param datafilePath Path to tsv input file to read
   * @param batchSize Number of rows from the tsv file to include
   * in a single activeMQ message payload
   * @throws Exception
   *
   * Builds the InputTsvFile object, reads in the input file, generates
   * all of the messages and stores them for later publishing
   */
  public InputTsvFile(final String datafilePath, final int batchSize)
      throws Exception {

    File dataFile = new File(datafilePath);
    BufferedReader tsv = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile), "UTF8"));
    String header = tsv.readLine();
    if (header != null) {
      parseHeader(header);
    } else {
      tsv.close();
      throw new Exception("The given tsv file: " + datafilePath + " is empty.");
    }
    System.out.println("Reading data from: " + datafilePath);
    String row = tsv.readLine();
    int rowNumber = 1;
    int batchMessageCount = 1;
    while (row != null) {
      parseRow(row, rowNumber);
      if (batchMessageCount < batchSize) {
        messages.add(createJsonMessage(rowNumber));
      } else {
        messages.add(createJsonMessage(rowNumber));
        batchMessage();
        batchMessageCount = 0;
      }
      row = tsv.readLine();
      rowNumber++;
      batchMessageCount++;
    }
    batchMessage();
    tsv.close();
    System.out.println("Read " + rowNumber + " rows from file: " + datafilePath);
  }

  /**
   * Returns a list of message payloads in JSON format to be
   * published via activeMQ
   *
   * @return List of activeMQ message payloads in JSON format
   */
  public List<String> getMessages() {
    return batchMessages;
  }


  /**
   * Takes all the messages queued in the messages list and
   * builds a single JSON-encoded message payload and stores
   * it in the batchMessages list.
   *
   * @throws Exception
   */
  private void batchMessage() throws Exception {
    if (!(messages.isEmpty())) {
      JsonFactory jsonFactory = new JsonFactory();
      StringWriter writer = new StringWriter();
      JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("process", "auth_demo");
      jsonGenerator.writeArrayFieldStart("data");
      for (String msg : messages) {
        jsonGenerator.writeRawValue(msg);
      }
      jsonGenerator.writeEndArray();
      jsonGenerator.writeEndObject();
      jsonGenerator.flush();
      jsonGenerator.close();

      batchMessages.add(writer.toString());
      messages.clear();
    }
  }

  /**
   * Top level method that parses the header from the tsv file
   *
   * @param header the entire header line from the tsv file
   * @throws Exception
   */
  private void parseHeader(String header) throws Exception {
    String[] fields = header.split("\t");
    Integer columnNumber = 0;
    for (String s : fields) {
      s = s.replaceAll("^\"|\"$", "");
      DataField field = new DataField(s);
      if (fieldNames.containsKey(field.name) != true) {
        fieldNames.put(field.name, field);
      }
      columns.put(columnNumber, fieldNames.get(field.name));
      columnNumber++;
    }
  }


  /**
   * Parses a single row from the tsv file
   *
   * @param row an entire row from the tsv file
   * @param rowNumber the row number from the tsv file
   * @throws Exception
   */
  private void parseRow(String row, int rowNumber) throws Exception {
    String[] fields = row.split("\t");
    int length = fields.length;
    int expectedLength = columns.size();
    if (length != expectedLength) {
      for (int i = 0; i < length; i++) {
          System.out.println(fields[i]);
      }
      throw new Exception ("Unexpected number of fields parsed from row: "
          + rowNumber + " expected " + expectedLength + " columns, "
          + " got " + length);
    }
    for (int i = 0; i < length; i++) {
      parseField(fields[i], new Integer(i), rowNumber);
    }
  }

  /**
   * Parses a single data field from the tsv file
   *
   * @param fieldString the actual value from the tsv file
   * @param columnNumber the column number from the tsv file
   * @param rowNumber the column number from the tsv file
   * @throws Exception
   */
  private void parseField(String fieldString, Integer columnNumber, int rowNumber) throws Exception {
    if (fieldString == null || fieldString.isEmpty()) {
      return;
    }
    DataField datafield = columns.get(columnNumber);
    if (PRIMITIVE_TYPES.contains(datafield.getType())) {
      datafield.addValue(fieldString);
    } else if (datafield.getType().equals("array")) {
      datafield.addValue(fieldString);
    } else if (datafield.getType().equals("map")) {
      String[] keyValue = fieldString.split(",");
      String keyString = keyValue[0].replaceAll("^\"|\"$", "");
      String valueString = keyValue[1].replaceAll("^\"|\"$", "");

      datafield.addMapValue(keyString, valueString);
    } else {
      throw new Exception("Unable to parse '" + fieldString + "' in column "
          + (columnNumber + 1) + " on row " + rowNumber);
    }
  }

  /**
   * Converts a single row from the tsv file into a JSON string
   *
   * @param rowNumber the row number from the tsv file
   * @return JSON string representing a row from the tsv file
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  private String createJsonMessage(int rowNumber) throws Exception {
    JsonFactory jsonFactory = new JsonFactory();
    StringWriter writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    jsonGenerator.writeStartObject();

    for (DataField datafield : fieldNames.values()) {
      if (datafield.getValue() == null) {
        continue;
      }
      if (PRIMITIVE_TYPES.contains(datafield.getType())) {
        if (datafield.getType().equals("date")) {
          jsonGenerator.writeNumberField(datafield.getName(), ((Date)datafield.getValue()).getTime());
        } else if (datafield.getType().equals("integer")) {
          jsonGenerator.writeNumberField(datafield.getName(), (Integer)datafield.getValue());
        } else if (datafield.getType().equals("long")) {
          jsonGenerator.writeNumberField(datafield.getName(), (Long)datafield.getValue());
        } else if (datafield.getType().equals("double")) {
          jsonGenerator.writeNumberField(datafield.getName(), (Double)datafield.getValue());
        } else if (datafield.getType().equals("json")) {
          jsonGenerator.writeFieldName(datafield.getName());
          jsonGenerator.writeRawValue((String)datafield.getValue());
        } else {
          jsonGenerator.writeStringField(datafield.getName(), (String)datafield.getValue());
        }
      } else if (datafield.getType().equals("array")) {
        String arrayType = datafield.getArrayType();
        jsonGenerator.writeFieldName(datafield.getName());
        jsonGenerator.writeStartArray();
        for (Object valueObject : ((List<Object>)datafield.getValue())) {
          if (arrayType.equals("date")) {
            jsonGenerator.writeNumber(((Date)valueObject).getTime());
          } else if (arrayType.equals("integer")) {
            jsonGenerator.writeNumber((Integer)valueObject);
          } else if (arrayType.equals("long")) {
            jsonGenerator.writeNumber((Long)valueObject);
          } else if (arrayType.equals("double")) {
            jsonGenerator.writeNumber((Double)valueObject);
          } else if (arrayType.equals("json")) {
            jsonGenerator.writeFieldName(datafield.getName());
            jsonGenerator.writeRaw((String)datafield.getValue());
          } else {
            jsonGenerator.writeString((String)valueObject);
          }
        }
        jsonGenerator.writeEndArray();
      } else if (datafield.getType().equals("map")) {
        String keyType = datafield.getKeyType();
        String valueType = datafield.getValueType();
        jsonGenerator.writeFieldName(datafield.getName());
        jsonGenerator.writeStartObject();

        for (Map.Entry<Object, Object> entry : ((Map<Object, Object>)datafield.getValue()).entrySet()) {
          String keyString = null;
          if (keyType.equals("date")) {
            keyString = "" + ((Date)entry.getKey()).getTime();
          } else {
            keyString = entry.getKey().toString();
          }
          Object valueObject = entry.getValue();

          if (valueType.equals("date")) {
            jsonGenerator.writeNumberField(keyString, ((Date)valueObject).getTime());
          } else if (valueType.equals("integer")) {
            jsonGenerator.writeNumberField(keyString, (Integer)valueObject);
          } else if (valueType.equals("long")) {
            jsonGenerator.writeNumberField(keyString, (Long)valueObject);
          } else if (valueType.equals("double")) {
            jsonGenerator.writeNumberField(keyString, (Double)valueObject);
          } else if (valueType.equals("json")) {
            jsonGenerator.writeFieldName(datafield.getName());
            jsonGenerator.writeRaw((String)datafield.getValue());
          } else {
            jsonGenerator.writeStringField(keyString, (String)valueObject);
          }
        }
        jsonGenerator.writeEndObject();
      } else {
        throw new Exception("Unable to generate json message for input row "
            + rowNumber);
      }
      //must clear out the data so that we the datafield is empty for the next row
      datafield.clearData();
    }

    jsonGenerator.flush();
    jsonGenerator.close();

    return writer.toString();
  }

  /**
   * Private inner class DataField
   *
   * This class represents the definition of one of the columns
   * from the tsv file. It contains the data type of the column,
   * and the value property serves as temporary storage for
   * collecting data for this column from a single row of the
   * file at at time.
   *
   * @author munwin
   *
   */
  private class DataField {
    String name = "";
    String type = "";
    Hashtable<String, Object> subtypes = new Hashtable<String, Object>();
    Object value = null;

    DataField(String field) throws Exception {
      String[] fieldParms = field.split("\\s*=\\s*", 2);
      if(fieldParms.length < 2) {
        throw new Exception("Column header can't be parsed. " +
            "Should be fieldname=datatype. Got: " + field);
      }
      this.name = fieldParms[0];
      parseType(fieldParms[1]);
    }

    /**
     * Clears the value property once the current row is processed
     */
    void clearData() {
      value = null;
    }


    /**
     * @return the name of the column
     */
    String getName() {
        return this.name;
    }


    /**
     * @return the datatype of the column
     */
    String getType() {
      return this.type;
    }

    /**
     * @return the value for the this field for the current row
     * The data type of the returned value could be:
     * Integer, Long, Double, String, Date, List, or Map
     */
    Object getValue() {
      return this.value;
    }

    /**
     * @return The data type for the array, if this DataField is an array
     */
    String getArrayType() {
        if (this.type.equals("array")){
          return subtypes.get("arraytype").toString();
        } else {
          return null;
        }
    }

    /**
     * @return The data type for the map key, if this DataField is a map
     */
    String getKeyType() {
      if (this.type.equals("map")){
        return subtypes.get("keytype").toString();
      } else {
        return null;
      }
    }

    /**
     * @return The data type for the map value, if this DataField is a map
     */
    String getValueType() {
      if (this.type.equals("map")){
        return subtypes.get("valuetype").toString();
      } else {
        return null;
      }
    }

    /**
     * Adds a value if the data type of this DataField is a primative or
     * an array
     *
     * @param valueString the value read from the tsv file
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    void addValue(String valueString) throws Exception {
      if (PRIMITIVE_TYPES.contains(this.type)) {
        if (this.type.equals("integer")) {
          this.value = Integer.parseInt(valueString);
        } else if (this.type.equals("long")) {
          this.value = Long.parseLong(valueString);
        } else if (this.type.equals("double")) {
          this.value = Double.parseDouble(valueString);
        } else if (this.type.equals("date")) {
          if (valueString.toLowerCase().startsWith("[now")) {
            this.value = getRelativeDate(valueString);
          } else {
            this.value = ((SimpleDateFormat)subtypes.get("date")).parse(valueString);
          }
        } else if (this.type.equals("json")) {
          this.value = valueString.replaceAll("^\"|\"$", "");
        } else {
          this.value = valueString;
        }
      } else if (this.type.equals("array")) {
        if (this.value == null) {
          this.value = new ArrayList<Object>();
        }
        ArrayList<Object> array = (ArrayList<Object>)this.value;
        String arraytype = subtypes.get("arraytype").toString();

        if (arraytype.equals("integer")) {
          array.add(Integer.parseInt(valueString));
        } else if (arraytype.equals("long")) {
          array.add(Long.parseLong(valueString));
        } else if (arraytype.equals("double")) {
          array.add(Double.parseDouble(valueString));
        } else if (arraytype.equals("date")) {
          if (valueString.toLowerCase().startsWith("[now")) {
            array.add(getRelativeDate(valueString));
          } else {
            array.add(((SimpleDateFormat)subtypes.get("arraydate")).parse(valueString));
          }
        } else {
          array.add(valueString);
        }
      } else {
        throw new Exception("Invalid data provided for data type: " + this.type);
      }
    }

    /**
     * Adds a map value if this DataType is a map
     *
     * @param keyString The key value read from the tsv file
     * @param valueString The value read from the tsv
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    void addMapValue(String keyString, String valueString) throws Exception {
      if (this.type.equals("map")) {
        if (this.value == null) {
          this.value = new Hashtable<Object, Object>();
        }
        Hashtable<Object, Object> map = (Hashtable<Object, Object>)this.value;

        Object keyObject = null;
        Object valueObject = null;

        String keytype = subtypes.get("keytype").toString();
        if (keytype.equals("integer")) {
          keyObject = Integer.parseInt(keyString);
        } else if (keytype.equals("long")) {
          keyObject = Long.parseLong(keyString);
        } else if (keytype.equals("double")) {
          keyObject = Double.parseDouble(keyString);
        } else if (keytype.equals("date")) {
          if (keyString.toLowerCase().startsWith("[now")) {
            keyObject = getRelativeDate(valueString);
          } else {
            keyObject = ((SimpleDateFormat)subtypes.get("keydate")).parse(keyString);
          }
        } else {
          keyObject = keyString;
        }

        String valuetype = subtypes.get("valuetype").toString();
        if (valuetype.equals("integer")) {
          valueObject = Integer.parseInt(valueString);
        } else if (valuetype.equals("long")) {
          valueObject = Long.parseLong(valueString);
        } else if (valuetype.equals("double")) {
          valueObject = Double.parseDouble(valueString);
        } else if (valuetype.equals("date")) {
          if (valueString.toLowerCase().startsWith("[now")) {
            valueObject = getRelativeDate(valueString);
          } else {
            valueObject = ((SimpleDateFormat)subtypes.get("valuedate")).parse(valueString);
          }
        } else {
          valueObject = valueString;
        }

        map.put(keyObject, valueObject);
      } else {
        throw new Exception("Invalid data provided for data type: " + this.type);
      }
    }

    /**
     * Parses the datatype portion of a header field
     *
     * @param typeString The data type portion of a header field
     * @throws Exception
     */
    void parseType(String typeString) throws Exception {
      String[] typeParms = typeString.split("\\(", 2);
      typeParms[0] = typeParms[0].replaceAll("\\s", "");
      if (FIELD_TYPES.contains(typeParms[0].toLowerCase())) {
        this.type = typeParms[0].toLowerCase();

        if (typeString.toLowerCase().matches("^date.*")) {
          this.type = "date";
          subtypes.put("date", parseDateFormat(typeString));
        }

        if (type.equals("array")) {;
        typeParms[1] = typeParms[1].replaceFirst("\\)$", "");
          if (PRIMITIVE_TYPES.contains(typeParms[1].toLowerCase())) {
            subtypes.put("arraytype", typeParms[1].toLowerCase());
          } else if (typeParms[1].toLowerCase().matches("^date.*")) {
            subtypes.put("arraytype", "date");
            subtypes.put("arraydate", parseDateFormat(typeParms[1]));
          } else {
            throw new Exception("Invalid array data type specified: " + typeString);
          }
        }
        if (type.equals("map")) {
          typeParms[1] = typeParms[1].replaceFirst("\\)$", "");
          String[] mapParms = typeParms[1].split(",\\s*", 2);
          if (PRIMITIVE_TYPES.contains(mapParms[0].toLowerCase())) {
            subtypes.put("keytype", mapParms[0].toLowerCase());
          } else if (mapParms[0].toLowerCase().matches("^date.*")) {
            subtypes.put("keytype", "date");
            subtypes.put("keydate", parseDateFormat(mapParms[0]));
          } else {
            throw new Exception("Invalid map data type specified: " + typeString);
          }
          if (PRIMITIVE_TYPES.contains(mapParms[1].toLowerCase())) {
            subtypes.put("valuetype", mapParms[1].toLowerCase());
          } else if (mapParms[1].toLowerCase().matches("^date.*")) {
            String[] mapTypeParms = mapParms[1].split("\\(", 2);
            subtypes.put("valuetype", "date");
            subtypes.put("valuedate", parseDateFormat(mapTypeParms[1]));
          } else {
            throw new Exception("Invalid map data type specified: " + typeString);
          }
        }
      } else {
        throw new Exception("Invalid data type specified: " + typeString);
      }
    }

    /**
     * Parses a date format from the header of the tsv file
     *
     * @param dateString a SimpleDateFormat format string
     * @return
     */
    SimpleDateFormat parseDateFormat(String dateString) {
      String[] dateParms = dateString.split("\\s*\\(\\s*\"*\\s*|\\s*\"*\\s*\\)+", 3);
      return new SimpleDateFormat(dateParms[1]);
    }

    /**
     * Figures out at date based on an offset
     *
     * @param offsetString Expects a string of the format [now OFFSET] where OFFSET is the number
     * of seconds from now (positive) or the number of seconds ago (negative).  For example:
     * [now 86400] would be 24 hours from now, where as [now -86400] would be 24 hours ago (yesterday)
     * @return A new date with the specified offset
     */
    Date getRelativeDate(String offsetString) {
      String offset = offsetString.toLowerCase().replaceAll("^\\[\\s*now\\s*|\\s*\\]\\s*$", "");
      if (offset.isEmpty()) {
        return new Date();
      } else {
        Date now = new Date();
        Long timeInMs = now.getTime();
        Long offsetSeconds = Long.parseLong(offset);
        return new Date(timeInMs + (offsetSeconds * 1000));
      }
    }
  }
}
