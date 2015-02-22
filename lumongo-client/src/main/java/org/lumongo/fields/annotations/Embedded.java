package org.lumongo.fields.annotations;

import org.lumongo.cluster.message.Lumongo.LMAnalyzer;

import java.lang.annotation.*;

/**
 * Specifics a field should be indexed
 * 
 * 
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface Embedded {



}
