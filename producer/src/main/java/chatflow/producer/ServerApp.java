package chatflow.producer;

import java.io.File;

import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;

import jakarta.websocket.server.ServerContainer;

public class ServerApp{
    public static void main(String[] args) throws Exception{
        Tomcat tomcat = new Tomcat();
        tomcat.setPort(8080);
        tomcat.setBaseDir(new File("target/tomcat").getAbsolutePath());

        Context context = tomcat.addContext("", new File(".").getAbsolutePath());

        Tomcat.addServlet(context, "default", new org.apache.catalina.servlets.DefaultServlet());
        context.addServletMappingDecoded("/", "default");

        tomcat.start();

        ServerContainer serverContainer =
                (ServerContainer) context.getServletContext()
                        .getAttribute("jakarta.websocket.server.ServerContainer");

        serverContainer.addEndpoint(SendEndPoint.class);

        System.out.println("Producer Service started.");

        tomcat.getServer().await();
    }
}
