package org.jouca.idfm_gtfs_rt.fetchers;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Scanner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.cdimascio.dotenv.Dotenv;

public class AlertFetcher {
    private static final Dotenv dotenv = Dotenv.configure().directory("/app").load();
    private static final String API_URL = "https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2";
    private static final String API_KEY = dotenv.get("API_KEY");

    public static JsonNode fetchAlertData() throws Exception {
        URI uri = new URI(API_URL);
        URL url = uri.toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept-Encoding", "application/json");
        conn.setRequestProperty("apiKey", API_KEY);

        InputStream responseStream = conn.getInputStream();
        String response;
        try (Scanner scanner = new Scanner(responseStream).useDelimiter("\\A")) {
            response = scanner.hasNext() ? scanner.next() : "";
        }

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readTree(response);
    }
}
