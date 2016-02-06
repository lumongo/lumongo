package org.lumongo.util;

import java.util.Collection;
import java.util.function.Consumer;

public class LumongoUtil {

	public static void handleLists(Object o, Consumer<? super Object> action) {
		if (o instanceof Collection) {
			Collection<?> c = (Collection<?>) o;
			c.stream().filter((obj) -> obj != null).forEach(action);
		}
		else if (o instanceof Object[]) {
			Object[] arr = (Object[]) o;
			for (Object obj : arr) {
				if (obj != null) {
					action.accept(action);
				}
			}
		}
		else {
			if (o != null) {
				action.accept(o);
			}
		}
	}
}
