package org.lumongo.fields.annotations;

import org.lumongo.cluster.message.LumongoIndex.SortAs.StringHandling;

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
@Repeatable(SortedFields.class)
public @interface Sorted {

	StringHandling stringHandling() default StringHandling.STANDARD;

	String fieldName() default "";
}
