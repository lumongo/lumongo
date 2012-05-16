package org.lumongo.admin.help;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import joptsimple.HelpFormatter;
import joptsimple.OptionDescriptor;

import org.lumongo.admin.ClusterAdmin;
import org.lumongo.admin.IndexAdmin;

public class LumongoHelpFormatter implements HelpFormatter {
	
	@Override
	public String format(Map<String, ? extends OptionDescriptor> options) {
		StringBuilder sb = new StringBuilder();
		sb.append("Usage:\n");
		for (String key : options.keySet()) {
			OptionDescriptor od = options.get(key);
			sb.append("    --");
			sb.append(od.options().iterator().next());
			if (!od.requiresArgument()) {
				sb.append(" <?>");
			}
			sb.append("\t\t");
			sb.append(od.description());
			
			String type = od.argumentTypeIndicator();
			if (type != null) {
				if (Integer.class.getCanonicalName().equals(type)) {
					
				}
				else if (IndexAdmin.Command.class.getName().equals(type)) {
					sb.append(" (values: ");
					sb.append(Arrays.toString(IndexAdmin.Command.values()));
					sb.append(")");
				}
				else if (ClusterAdmin.Command.class.getName().equals(type)) {
					sb.append(" (values: ");
					sb.append(Arrays.toString(ClusterAdmin.Command.values()));
					sb.append(")");
				}
				
			}
			List<?> values = od.defaultValues();
			if (values != null && !values.isEmpty()) {
				sb.append(" (default: ");
				if (values.size() == 1) {
					sb.append(values.get(0));
				}
				else {
					sb.append(values);
				}
				sb.append(")");
			}
			
			if (od.isRequired()) {
				sb.append(" (required)");
			}
			
			sb.append("\n");
		}
		return sb.toString();
	}
}
