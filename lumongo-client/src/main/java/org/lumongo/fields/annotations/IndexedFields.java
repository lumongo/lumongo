package org.lumongo.fields.annotations;

import java.lang.annotation.*;

/**
 * Specifics a field should be indexed multiple ways
 *
 *
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface IndexedFields {
	Indexed[] value();
}
