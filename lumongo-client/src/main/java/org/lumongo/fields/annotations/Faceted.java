package org.lumongo.fields.annotations;

import org.lumongo.cluster.message.Lumongo;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@Repeatable(FacetedFields.class)
public @interface Faceted {

	Lumongo.FacetAs.DateHandling dateHandling() default Lumongo.FacetAs.DateHandling.DATE_YYYY_MM_DD;

	String name() default "";
}
