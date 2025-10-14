package org.jouca.idfm_gtfs_rt.fetchers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;
import io.github.cdimascio.dotenv.Dotenv;

/**
 * Fetcher for SIRI Lite real-time estimated timetable data from Île-de-France Mobilités API.
 * 
 * <p>This class provides functionality to retrieve real-time transit information in SIRI Lite format,
 * which includes estimated arrival and departure times for public transportation in the Île-de-France region.
 * The data is fetched from the PRIM (Platform for Real-time Information on Mobility) marketplace API.</p>
 * 
 * <p>The fetcher handles:</p>
 * <ul>
 *   <li>HTTP connections with appropriate timeouts</li>
 *   <li>GZIP compression for response data</li>
 *   <li>API authentication using an API key</li>
 *   <li>JSON parsing of the response</li>
 * </ul>
 * 
 * @see <a href="https://prim.iledefrance-mobilites.fr/">PRIM API Documentation</a>
 * 
 * @author Jouca
 * @since 1.0
 */
public class SiriLiteFetcher {
    /**
     * Environment configuration loader for accessing environment variables.
     * Configured to load .env file from the /app directory (Docker container path).
     */
    private static final Dotenv dotenv = Dotenv.configure().directory("/app").load();
    
    /**
     * API endpoint URL for fetching estimated timetable data.
     * The LineRef=ALL parameter retrieves data for all transit lines in the network.
     */
    private static final String API_URL = "https://prim.iledefrance-mobilites.fr/marketplace/estimated-timetable?LineRef=ALL";
    
    /**
     * API key for authenticating requests to the PRIM API.
     * Retrieved from environment variables for security purposes.
     */
    private static final String API_KEY = dotenv.get("API_KEY");

    /**
     * Fetches real-time estimated timetable data from the SIRI Lite API.
     * 
     * <p>This method performs the following operations:</p>
     * <ol>
     *   <li>Establishes an HTTP GET connection to the PRIM API endpoint</li>
     *   <li>Sets appropriate timeout values (5s connect, 20s read)</li>
     *   <li>Adds required headers including API key authentication</li>
     *   <li>Handles GZIP-compressed responses automatically</li>
     *   <li>Parses the JSON response into a JsonNode structure</li>
     * </ol>
     * 
     * <p><b>API Request Configuration:</b></p>
     * <ul>
     *   <li>Method: GET</li>
     *   <li>Connect Timeout: 5,000 ms</li>
     *   <li>Read Timeout: 20,000 ms</li>
     *   <li>Accept: application/json</li>
     *   <li>Accept-Encoding: gzip</li>
     *   <li>Authentication: API key via custom header</li>
     * </ul>
     * 
     * @return JsonNode containing the parsed SIRI Lite data structure with estimated timetables
     *         for all transit lines in the Île-de-France network
     * @throws Exception if any error occurs during:
     *         <ul>
     *           <li>Network connection establishment</li>
     *           <li>HTTP request/response handling</li>
     *           <li>Stream decompression (if GZIP encoded)</li>
     *           <li>JSON parsing</li>
     *         </ul>
     * @see JsonNode
     * @see ObjectMapper#readTree(String)
     */
    @SuppressWarnings("deprecation")
    public static JsonNode fetchSiriLiteData() throws Exception {
        // Establish connection to the SIRI Lite API endpoint
        URL url = new URL(API_URL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        
        // Set timeout values to prevent indefinite hanging
        conn.setConnectTimeout(5_000);  // 5 seconds to establish connection
        conn.setReadTimeout(20_000);     // 20 seconds to read response data
        
        // Configure request headers
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Accept-Encoding", "gzip");  // Request compressed response
        conn.setRequestProperty("apiKey", API_KEY);           // API authentication

        // Get the response stream
        InputStream responseStream = conn.getInputStream();
        
        // Handle GZIP compression if present in the response
        String encoding = conn.getContentEncoding();
        if (encoding != null && encoding.equalsIgnoreCase("gzip")) {
            responseStream = new java.util.zip.GZIPInputStream(responseStream);
        }
        
        // Read the entire response body into a string
        Scanner scanner = new Scanner(responseStream).useDelimiter("\\A");
        String response = scanner.hasNext() ? scanner.next() : "";
        scanner.close();

        // Parse the JSON response and return as JsonNode
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readTree(response);
    }
}
