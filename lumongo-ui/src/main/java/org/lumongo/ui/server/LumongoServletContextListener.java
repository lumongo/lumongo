package org.lumongo.ui.server;

import org.lumongo.ui.server.filter.GWTCacheControlFilter;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.util.EnumSet;

@WebListener
public class LumongoServletContextListener implements ServletContextListener {

	@Override
	public void contextInitialized(ServletContextEvent sce) {

		String appPrefix = getAppPrefix();

		ServletContext servletContext = sce.getServletContext();

		setServletParameters(servletContext);
		setFilters(servletContext);

		servletContext.addServlet("uiqueryservice", UIQueryServiceImpl.class).addMapping("/" + appPrefix + "/" + "uiqueryservice");

	}

	public void setFilters(ServletContext servletContext) {
		FilterRegistration.Dynamic gwtcachecontrolfilter = servletContext.addFilter("gwtcachecontrolfilter", GWTCacheControlFilter.class);
		gwtcachecontrolfilter.setAsyncSupported(true);
		gwtcachecontrolfilter.addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST, DispatcherType.ASYNC), false, "/*");
	}

	public void setServletParameters(ServletContext servletContext) {
		servletContext.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {

	}

	public String getAppPrefix() {
		return "lumongoui";
	}

}
