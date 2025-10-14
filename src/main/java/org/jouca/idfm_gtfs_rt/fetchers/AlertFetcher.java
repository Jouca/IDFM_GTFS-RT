package org.jouca.idfm_gtfs_rt.fetchers;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Scanner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Fetcher class responsible for retrieving real-time alert and disruption data
 * from the Île-de-France Mobilités API.
 * <p>
 * This class provides functionality to fetch disruption information for the IDFM
 * public transportation network, which can be used to generate GTFS-RT service alerts.
 * </p>
 * 
 * @author Jouca
 * @since 1.0
 */
public class AlertFetcher {
    /**
     * Dotenv instance configured to load environment variables from the /app directory.
     * This is typically used in Docker containerized environments.
     */
    private static final Dotenv dotenv = Dotenv.configure().directory("/app").load();
    
    /**
     * The API endpoint URL for fetching disruptions from the IDFM marketplace.
     * This endpoint provides bulk disruption data in version 2 format.
     */
    private static final String API_URL = "https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2";
    
    /**
     * The API key required for authentication with the IDFM API.
     * This key is loaded from environment variables for security purposes.
     */
    private static final String API_KEY = dotenv.get("API_KEY");

    /**
     * Fetches real-time alert and disruption data from the IDFM API.
     * <p>
     * This method performs an HTTP GET request to the IDFM disruptions API endpoint,
     * authenticates using the configured API key, and returns the parsed JSON response
     * as a JsonNode object for further processing.
     * </p>
     * 
     * @return JsonNode containing the alert and disruption data from the API
     * @throws Exception if there's an error during the HTTP request, network connection,
     *                   or JSON parsing. Possible exceptions include:
     *                   <ul>
     *                     <li>URISyntaxException - if the API URL is malformed</li>
     *                     <li>IOException - if there's a network or connection error</li>
     *                     <li>JsonProcessingException - if the response cannot be parsed as JSON</li>
     *                   </ul>
     */
    public static JsonNode fetchAlertData() throws Exception {
        // Convert the API URL string to a URI and then to a URL object
        URI uri = new URI(API_URL);
        URL url = uri.toURL();
        
        // Establish HTTP connection to the API endpoint
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        
        // Set request headers for JSON response and API authentication
        conn.setRequestProperty("Accept-Encoding", "application/json");
        conn.setRequestProperty("apiKey", API_KEY);

        // Read the response stream from the API
        InputStream responseStream = conn.getInputStream();
        String response;
        
        // Use Scanner with delimiter to read the entire response as a single string
        try (Scanner scanner = new Scanner(responseStream).useDelimiter("\\A")) {
            response = scanner.hasNext() ? scanner.next() : "";
        }

        // Parse the JSON response string into a JsonNode tree structure
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readTree(response);
    }
}
