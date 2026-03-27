package chatflow.db.metrics;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class MetricsServlet extends HttpServlet {

  private final AnalyticsService analytics;
  private final ObjectMapper mapper;

  public MetricsServlet(AnalyticsService analytics) {
    this.analytics = analytics;
    this.mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String path = req.getServletPath();
    if ("/health".equals(path)) {
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write("OK");
      return;
    }

    resp.setContentType("application/json");
    resp.setCharacterEncoding("UTF-8");
    resp.setStatus(HttpServletResponse.SC_OK);
    resp.setHeader("Access-Control-Allow-Origin", "*");

    try {
      Map<String, Object> metrics = analytics.getMetrics();
      mapper.writeValue(resp.getWriter(), metrics);
    } catch (Exception e) {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      resp.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
    }
  }
}
