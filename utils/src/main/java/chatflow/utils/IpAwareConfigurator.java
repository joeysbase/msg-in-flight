package chatflow.utils;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.websocket.HandshakeResponse;
import jakarta.websocket.server.HandshakeRequest;
import jakarta.websocket.server.ServerEndpointConfig;

public class IpAwareConfigurator extends ServerEndpointConfig.Configurator {

    @Override
    public void modifyHandshake(ServerEndpointConfig config,
                                HandshakeRequest request,
                                HandshakeResponse response) {

        HttpServletRequest httpRequest =
                (HttpServletRequest) request.getHttpSession();

        if (httpRequest != null) {
            String clientIp = extractClientIp(httpRequest);
            config.getUserProperties().put("client-ip", clientIp);
        }
    }

    private String extractClientIp(HttpServletRequest request) {

        // 1️⃣ X-Forwarded-For (most common)
        String xff = request.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            // First IP in chain = original client
            return xff.split(",")[0].trim();
        }

        // 2️⃣ X-Real-IP (nginx alternative)
        String realIp = request.getHeader("X-Real-IP");
        if (realIp != null && !realIp.isBlank()) {
            return realIp;
        }

        // 3️⃣ Direct connection fallback
        return request.getRemoteAddr();
    }
}
