package in.apurv;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData.Array;
import java.util.Collection;

public class Converter {
    public static GenericRecord convertToGenericRecord(Object value, Schema schema) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            Object fieldValue = null;

            try {
                fieldValue = value.getClass().getMethod("get" + Character.toUpperCase(fieldName.charAt(0)) +
                        fieldName.substring(1)).invoke(value);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (fieldValue != null) {
                if (fieldSchema.getType().equals(Schema.Type.ARRAY)) {
                    if (fieldValue instanceof Collection<?>) {
                        Collection<?> collection = (Collection<?>) fieldValue;
                        GenericArray<Object> array = new GenericData.Array<>(collection.size(), fieldSchema.getElementType());
                        for (Object item : collection) {
                            if (item != null) {
                                Object convertedItem = convertNestedObject(item, fieldSchema.getElementType());
                                array.add(convertedItem);
                            }
                        }
                        genericRecord.put(fieldName, array);
                    }
                } else {
                    Object convertedValue = convertNestedObject(fieldValue, fieldSchema);
                    genericRecord.put(fieldName, convertedValue);
                }
            }
        }
        return genericRecord;
    }



    private static Object convertNestedObject(Object fieldValue, Schema fieldSchema) {

        Object convertedValue;
        if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
            convertedValue = convertToGenericRecord(fieldValue, fieldSchema);
        } else {
            convertedValue = fieldValue;
        }
        return convertedValue;
    }
}
