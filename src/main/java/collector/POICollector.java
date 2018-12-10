package collector;

import com.google.gson.Gson;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.sql.*;

public class POICollector extends AbstractCollector {
    int rank = 10;  // next
    String results;
    double C = 0.1;

    double x0 = 116.3978;
    double y0 = 39.9033;
    double dx = 0;
    double dy = 0;
    double nextdx = 0.01;
    double nextdy = 0.01;

    class POI implements Serializable {
        String name;
        double lat, lon;
        double price, rating;
        public POI(String poiStr) {
            String[] data = poiStr.split(" ");
            name = data[0];
            lon = Double.parseDouble(data[1]);
            lat = Double.parseDouble(data[2]);
            price = Double.parseDouble(data[3]);
            rating = Double.parseDouble(data[4]);
        }
        String getString() {
            return name + " " + String.valueOf(lon) +  " " + String.valueOf(lat);
        }
    }

    public POICollector(int second) {
        /* load the driver */
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println(ex);
            return;
        }
        this.interval = second;
    }

    public POICollector(int second, double lon, double lat) {
        this(second);
        this.x0 = lon;
        this.y0 = lat;
    }

    public String results() {
        return results;
    }

    public List<String> getAndProcess() {
        String USER_AGENT =
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.112 Safari/535.1";
        List<String> poiStrings = new LinkedList<>();

        java.sql.Connection c = null;
        PreparedStatement preparedStmt = null;
        ResultSet rs = null;

        try {
            /* create an instance of a Connection object */
            String user = "root";
            String password = "19950809";
            c = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/SpatialData?characterEncoding=utf8", user, password);

            String nextPolygonText = generatePolygon(x0, y0, nextdx, nextdy);
            String polygonText = generatePolygon(x0, y0, dx, dy);
            preparedStmt = c.prepareStatement(
                    "SELECT name, AsText(location), price, score FROM POI WHERE MBRContains(GeomFromText('"
                            + nextPolygonText + "'), location) AND NOT MBRContains(GeomFromText('"
                            + polygonText + "'), location)");

            rs = preparedStmt.executeQuery();
            String name = "";
            String location = "";
            String rating = "";
            String price = "";
            int cnt = 0;
            while (rs.next()) {
                name = rs.getString("name");
                location = parseLocation(rs.getString("AsText(location)"));
                rating = rs.getString("score");
                price = rs.getString("price");
                String poiStr = name + " " + location + " " + price + " " + rating;
                //System.out.println(poiStr);
                poiStrings.add(poiStr);
                cnt++;
            }
            // Update next scope
            dx = nextdx; dy = nextdy;
            if (cnt != 0) {
                nextdx += C / cnt;
                nextdy += C / cnt;
            }
            else {
                nextdx += 0.1;
                nextdy += 0.1;
            }
            //System.out.println("dx: " + dx + ", nextdx: " + nextdx);
            //System.out.println("Count: " + cnt);
        } catch (SQLException ex) {
            System.out.println("SQLException caught");
            System.out.println("---");
            while ( ex != null ) {
                System.out.println("Message   : " + ex.getMessage());
                System.out.println("SQLState  : " + ex.getSQLState());
                System.out.println("ErrorCode : " + ex.getErrorCode());
                System.out.println("---");
                ex = ex.getNextException();
            }
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {

        }
        return poiStrings;
    }

    public void postProcess(JavaReceiverInputDStream<String> lines) {
        JavaDStream<POI> pois = lines.window(Durations.minutes(10)).map(s -> new POI(s));
        JavaPairDStream<String, Double> pairs = pois
                .mapToPair(poi -> new Tuple2<String, Double>(poi.getString(), poi.rating));
        JavaPairDStream<Double, String> order = pairs
                .mapToPair(Tuple2::swap)
                .transformToPair(s -> s.sortByKey(false));
        order.foreachRDD(rdd -> {
            List result = rdd.take(rank);
            results = new Gson().toJson(result);
            System.out.println(results);
        });
    }

    private void test() {
        java.sql.Connection c = null;
        PreparedStatement preparedStmt = null;
        ResultSet rs = null;
        try {
//            String polygonText = generatePolygon(x0, y0, nextdx, nextdy);
//            preparedStmt = c.prepareStatement(
//                    "SELECT * FROM POI where MBRContains(GeomFromText('"+ polygonText +"'), location)");
            String nextPolygonText = generatePolygon(x0, y0, nextdx, nextdy);
            String polygonText = generatePolygon(x0, y0, dx, dy);
            preparedStmt = c.prepareStatement(
                    "SELECT name, AsText(location), price, score FROM POI WHERE MBRContains(GeomFromText('"+ nextPolygonText +"'), location) AND " +
                            "NOT MBRContains(GeomFromText('"+ polygonText +"'), location)");

            rs = preparedStmt.executeQuery();
            String name = "";
            String location = "";
            String score = "";
            String price = "";
            int cnt = 0;
            while (rs.next()) {
                name = rs.getString("name");
                location = parseLocation(rs.getString("AsText(location)"));
                score = rs.getString("score");
                price = rs.getString("price");
                String poiStr = name + " " + location + " " + price + " " + score;
                System.out.println(poiStr);
                cnt++;
            }
            System.out.println("Count: " + cnt);
        } catch (SQLException ex) {
            System.out.println("SQLException caught");
            System.out.println("---");
            while ( ex != null ) {
                System.out.println("Message   : " + ex.getMessage());
                System.out.println("SQLState  : " + ex.getSQLState());
                System.out.println("ErrorCode : " + ex.getErrorCode());
                System.out.println("---");
                ex = ex.getNextException();
            }
        } finally {
            try { rs.close(); } catch (Exception e) { /* ignored */ }
            try { preparedStmt.close(); } catch (Exception e) { /* ignored */ }
            try { c.close(); } catch (Exception e) { /* ignored */ }
        }
    }

    private String generatePolygon(double x0, double y0, double dx, double dy) {
        String polygon = "Polygon((";
        double x1 = x0-dx, x2 = x0+dx, y1 = y0-dx, y2 = y0+dx;
        polygon += String.valueOf(x1) + " " + String.valueOf(y1) + ",";
        polygon += String.valueOf(x2) + " " + String.valueOf(y1) + ",";
        polygon += String.valueOf(x2) + " " + String.valueOf(y2) + ",";
        polygon += String.valueOf(x1) + " " + String.valueOf(y2) + ",";
        polygon += String.valueOf(x1) + " " + String.valueOf(y1) + "))";
        return polygon;
    }

    private String parseLocation(String pointText) {
        return pointText.substring(6, pointText.length() - 1);
    }

    public static void main(String[] args) {
        POICollector collector = new POICollector(3);
        //collector.test();

        List<String> lists = collector.getAndProcess();
        System.out.println(lists);
    }
}