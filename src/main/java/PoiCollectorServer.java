import collector.AbstractCollector;
import collector.POICollector;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;

public class PoiCollectorServer {

    static AbstractCollector collector = null;
    static int second = 3;

    public static void main(String[] args) throws Exception {
        setupServer();
    }

    public static void setupServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8504), 0);
        HttpContext poiContext = server.createContext("/Poi");  // context for crawler application
        poiContext.setHandler(exchange -> {
            try {
                poiCollectorHandle(exchange);
            }
            catch (InterruptedException ex) {
                System.err.println("InterruptedException!");
                throw new RuntimeException();
            }
        });
        server.start();
        System.out.println("Server starts");
    }

    public static void poiCollectorHandle(HttpExchange exchange)
            throws IOException, InterruptedException {
        String requestMethod = exchange.getRequestMethod();
        System.out.println(exchange.getRequestURI().getQuery());
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        if (requestMethod.equalsIgnoreCase("GET")) {
            URI requestedUri = exchange.getRequestURI();
            Map<String, String> parameters = splitQuery(requestedUri);
            if (parameters.containsKey("start")) {
                int start = Integer.parseInt(parameters.get("start"));
                if (start == 1) {  // start a new collector
                    if (!parameters.containsKey("lon") || !parameters.containsKey("lat")) {
                        errorHandle(exchange, "Needs initial coordinate!");
                        return;
                    }
                    double lon = Double.parseDouble(parameters.get("lon"));
                    double lat = Double.parseDouble(parameters.get("lat"));
                    if (collector != null) {
                        errorHandle(exchange, "POI Collector already started!");
                        return;
                    }
                    collector = new POICollector(second, lon, lat);
                    new Thread(() -> {
                        try {
                            collector.run();
                        }
                        catch (InterruptedException ex) {
                            System.err.println("InterruptedException!");
                            throw new RuntimeException();
                        }
                    }).start();
                    msgHandle(exchange,"Success!");
                }
                else if (start == 0) {  // stop the crawler
                    if (collector == null) {
                        errorHandle(exchange, "POI Collector NOT started yet!");
                        return;
                    }
                    collector.stop();  // stop previous collector
                    msgHandle(exchange,"Successfully stop crawler!");
                }
                else {  // request data from the running collect
                    if (collector == null) {
                        errorHandle(exchange, "POI Collector NOT started yet!");
                        return;
                    }
                    String jsonString = collector.results();
                    if (jsonString == null)
                        jsonString = "";
                    System.out.println(jsonString);
                    byte[] response = jsonString.getBytes("UTF-8");
                    exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
                    exchange.sendResponseHeaders(200, response.length); //results code and length
                    OutputStream os = exchange.getResponseBody();
                    os.write(response);
                    os.close();
                }
            }
            else
                errorHandle(exchange, "Needs start parameter!");
        }
    }

    private static void msgHandle(HttpExchange exchange, String msg) throws IOException {
        String response = msg;
        exchange.sendResponseHeaders(200, response.getBytes().length); //results code and length
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }

    private static void errorHandle(HttpExchange exchange, String msg) throws IOException {
        String response = "Invalid Request! " + msg;
        exchange.sendResponseHeaders(400, response.getBytes().length); //results code and length
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }

    public static Map<String, String> splitQuery(URI uri) throws UnsupportedEncodingException {
        Map<String, String> query_pairs = new LinkedHashMap<String, String>();
        String query = uri.getQuery();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"),
                    URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
        }
        return query_pairs;
    }
}
