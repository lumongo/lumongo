package org.lumongo.fields.annotations;

import java.lang.annotation.*;

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
@Repeatable(IndexedFields.class)
public @interface Indexed {
	/**
	 * Sets the analyzer to use to index the field
	 * 
	 */
	LMAnalyzer analyzer();
	
	String fieldName() default "";
}
