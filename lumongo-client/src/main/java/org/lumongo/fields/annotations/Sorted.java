package org.lumongo.fields.annotations;

import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.FacetAs.LMFacetType;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface Sorted {
	
	Lumongo.SortAs.SortType type();
	
	String fieldName() default "";
}
