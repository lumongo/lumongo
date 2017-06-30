package org.lumongo.fields.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.lumongo.cluster.message.LumongoIndex.FacetAs;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@Repeatable(FacetedFields.class)
public @interface Faceted {

	FacetAs.DateHandling dateHandling() default FacetAs.DateHandling.DATE_YYYY_MM_DD;

	String name() default "";
}
