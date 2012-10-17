package org.lumongo.fields;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;

import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMField;

public class IndexedFieldInfo<T> {
    private final String fieldName;
    private final Field field;
    private final LMAnalyzer lmAnalyzer;

    public IndexedFieldInfo(Field field, String fieldName, LMAnalyzer lmAnalyzer) {
        this.fieldName = fieldName;
        this.field = field;
        this.lmAnalyzer = lmAnalyzer;
    }

    public LMAnalyzer getLMAnalyzer() {
        return lmAnalyzer;
    }

    public String getFieldName() {
        return fieldName;
    }

    public LMField build(T object) throws IllegalArgumentException, IllegalAccessException {

        if (object != null) {
            LMField.Builder lmFieldBuilder = LMField.newBuilder();
            lmFieldBuilder.setFieldName(fieldName);
            Object o = field.get(object);

            if (o != null) {
                if (o instanceof Collection<?>) {
                    Collection<?> l = (Collection<?>) o;
                    for (Object s : l) {
                        addObjectData(lmFieldBuilder, s);
                    }
                }
                else if (o.getClass().isArray()) {
                    Object[] l = (Object[]) o;
                    for (Object s : l) {
                        addObjectData(lmFieldBuilder, s);
                    }
                }
                else {
                    addObjectData(lmFieldBuilder, o);
                }

                return lmFieldBuilder.build();
            }
        }
        return null;
    }

    private void addObjectData(LMField.Builder lmFieldBuilder, Object o) {
        if (LMAnalyzer.NUMERIC_INT.equals(lmAnalyzer)) {
            lmFieldBuilder.addIntValue((Integer) o);
        }
        else if (LMAnalyzer.NUMERIC_LONG.equals(lmAnalyzer)) {
            if (o instanceof Long) {
                lmFieldBuilder.addLongValue((Long) o);
            }
            else if (o instanceof Date) {
                lmFieldBuilder.addLongValue(((Date) o).getTime());
            }
            else {
                throw new RuntimeException("Unsupported type for field: " + fieldName + " with analyzer " + lmAnalyzer + ": use types Long or Date");
            }
        }
        else if (LMAnalyzer.NUMERIC_FLOAT.equals(lmAnalyzer)) {
            lmFieldBuilder.addFloatValue((Float) o);
        }
        else if (LMAnalyzer.NUMERIC_DOUBLE.equals(lmAnalyzer)) {
            lmFieldBuilder.addDoubleValue((Double) o);
        }
        else {
            if (o instanceof String) {
                lmFieldBuilder.addFieldValue((String) o);
            }
            else {
                throw new RuntimeException("Unsupported type for field: " + fieldName + " with analyzer " + lmAnalyzer + ": use type String");
            }
        }
    }
}
