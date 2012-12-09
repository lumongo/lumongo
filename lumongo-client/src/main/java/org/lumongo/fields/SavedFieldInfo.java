package org.lumongo.fields;

import java.lang.reflect.Field;

import org.lumongo.util.CommonCompression;

import com.mongodb.DBObject;

public class SavedFieldInfo<T> {
    private final String fieldName;
    private final Field field;
    private boolean compressed;

    public SavedFieldInfo(Field field, String fieldName, boolean compressed) {
        this.fieldName = fieldName;
        this.field = field;
        this.compressed = compressed;
    }

    public String getFieldName() {
        return fieldName;
    }

    public boolean isCompressed() {
        return compressed;
    }

    Object getValue(T object) throws Exception {

        Object o = field.get(object);

        if (compressed) {
            if (String.class.equals(field.getType())) {
                String s = (String) o;
                o = CommonCompression.compressZlib(s.getBytes("UTF-8"), CommonCompression.CompressionLevel.NORMAL);
            }
            else if (byte[].class.equals(field.getType())) {
                byte[] b = (byte[]) o;
                o = CommonCompression.compressZlib(b, CommonCompression.CompressionLevel.NORMAL);
            }
        }

        return o;
    }

    public void populate(T newInstance, DBObject savedDBObject) throws Exception {

        Object value = savedDBObject.get(fieldName);
        if (compressed) {
            if (value instanceof byte[]) {
                byte[] b = (byte[]) value;
                if (String.class.equals(field.getType())) {
                    field.set(newInstance, new String(CommonCompression.uncompressZlib(b), "UTF-8"));
                    return;
                }
                else if (byte[].class.equals(field.getType())) {
                    field.set(newInstance, CommonCompression.uncompressZlib(b));
                    return;
                }

            }

        }
        field.set(newInstance, value);
    }
}
