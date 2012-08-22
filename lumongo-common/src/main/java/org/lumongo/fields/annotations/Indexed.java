package org.lumongo.fields.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.lumongo.cluster.message.Lumongo.LMAnalyzer;

/**
 * Specifics a field should be indexed
 * 
 * 
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface Indexed {
	/**
	 * Sets the analyzer to use to index the field
	 * 
	 */
	LMAnalyzer value();
}
