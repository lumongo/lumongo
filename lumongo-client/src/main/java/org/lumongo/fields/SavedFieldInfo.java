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

        if (compressed) {
            if (object instanceof String) {
                String s = (String) object;
                return CommonCompression.compressZlib(s.getBytes("UTF-8"), CommonCompression.CompressionLevel.NORMAL);
            }
            // else ?
        }

        return field.get(object);
    }

    public void populate(T newInstance, DBObject savedDBObject) throws Exception {
        Object value = savedDBObject.get(fieldName);
        if (compressed) {
            if (value instanceof byte[]) {
                byte[] b = (byte[]) value;
                field.set(newInstance, new String(CommonCompression.uncompressZlib(b), "UTF-8"));
                return;
            }
            // else ?
        }
        field.set(newInstance, value);
    }
}
