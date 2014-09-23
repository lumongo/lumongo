package org.lumongo.fields.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.lumongo.cluster.message.Lumongo.FacetAs.LMFacetType;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface Faceted {
	
	LMFacetType type() default LMFacetType.STANDARD;
	
	String name() default "";
}
