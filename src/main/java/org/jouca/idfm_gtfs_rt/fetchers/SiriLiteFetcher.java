package org.jouca.idfm_gtfs_rt.fetchers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;
import io.github.cdimascio.dotenv.Dotenv;

public class SiriLiteFetcher {
    private static final Dotenv dotenv = Dotenv.configure().directory("/app").load();
    private static final String API_URL = "https://prim.iledefrance-mobilites.fr/marketplace/estimated-timetable?LineRef=ALL";
    private static final String API_KEY = dotenv.get("API_KEY");

    @SuppressWarnings("deprecation")
    public static JsonNode fetchSiriLiteData() throws Exception {
        URL url = new URL(API_URL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5_000);
        conn.setReadTimeout(20_000);
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Accept-Encoding", "gzip");
        conn.setRequestProperty("apiKey", API_KEY);

        InputStream responseStream = conn.getInputStream();
        String encoding = conn.getContentEncoding();
        if (encoding != null && encoding.equalsIgnoreCase("gzip")) {
            responseStream = new java.util.zip.GZIPInputStream(responseStream);
        }
        Scanner scanner = new Scanner(responseStream).useDelimiter("\\A");
        String response = scanner.hasNext() ? scanner.next() : "";
        scanner.close();

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readTree(response);
    }
}
