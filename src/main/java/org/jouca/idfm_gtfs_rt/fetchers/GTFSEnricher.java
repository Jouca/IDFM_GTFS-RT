package org.jouca.idfm_gtfs_rt.fetchers;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.*;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.*;

/**
 * Enriches a GTFS ZIP file's {@code stops.txt} with data sourced from the IDFM
 * NeTEx API (PRIM marketplace).
 *
 * <p>Two operations are performed:
 * <ol>
 *   <li><b>Enrich existing stops</b>: fills the {@code platform_code} column
 *       for stops whose {@code stop_id} is {@code IDFM:{digits}} using the
 *       {@code PublicCode} field from the NeTEx Quay element.</li>
 *   <li><b>Create missing stops</b>: for each NeTEx Quay whose arrid has no
 *       corresponding GTFS stop, a new row is appended using coordinates and
 *       name from the NeTEx data and the {@code parent_station} from the
 *       Quay's {@code ParentZoneRef} (monomodalStopPlace).</li>
 * </ol>
 *
 * <p>Coordinates in the NeTEx file are in Lambert 93 (EPSG:2154) and are
 * converted to WGS84 via the inverse Lambert Conformal Conic projection.
 */
public class GTFSEnricher {

    private static final Logger logger = LoggerFactory.getLogger(GTFSEnricher.class);

    private static final Dotenv dotenv = Dotenv.configure().directory("/app").ignoreIfMissing().load();

    static final String NETEX_URL =
        "https://prim.iledefrance-mobilites.fr/marketplace/icar/getData" +
        "?method=getAll&GeneralGroupOfEntities=true&multimodalStopPlace=true" +
        "&monomodalStopPlace=true&Quay_FR1=true&Quay_LOC=true" +
        "&StopPlaceEntrance=true&destinations=true";

    private static final Pattern PURE_NUMERIC_STOP_ID = Pattern.compile("^IDFM:(\\d+)$");
    private static final Pattern STATION_NAME_LETTERS = Pattern.compile("[a-zA-ZÀ-ÿ]{4,}");
    private static final Pattern ARTNAME_PLATFORM = Pattern.compile(
        "(?:voie|quai)\\s+([A-Za-z0-9]{1,3})\\s*$", Pattern.CASE_INSENSITIVE);

    // Extracts the numeric ID from NeTEx IDs like "FR::Quay:12345:FR1"
    private static final Pattern NETEX_ID = Pattern.compile(":(\\d+):[A-Z0-9]+$");
    // Extracts the zone number from "FR1:TariffZone:4:LOC"
    private static final Pattern TARIFF_ZONE_NUM = Pattern.compile("TariffZone:(\\d+):");

    private static final String NS = "http://www.netex.org.uk/netex";
    private static final String GML_NS = "http://www.opengis.net/gml/3.2";

    // -------------------------------------------------------------------------
    // Data model
    // -------------------------------------------------------------------------

    record QuayData(String name, String lat, String lon,
                    String tariffZone, String zdcid, String publicCode) {}

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Produces an enriched GTFS ZIP at {@code outputZipPath} from the original
     * GTFS ZIP at {@code inputZipPath}.
     *
     * @param inputZipPath  path to the original GTFS ZIP (e.g. IDFM-gtfs.zip)
     * @param outputZipPath path for the enriched output ZIP
     * @throws IOException on network or file errors
     */
    public static void enrichGTFS(String inputZipPath, String outputZipPath) throws IOException {
        logger.info("Starting GTFS enrichment (platform_code + missing stops)…");

        Map<String, QuayData> quayData = new LinkedHashMap<>();
        Map<String, String> arridToZdcid = new HashMap<>();
        fetchNeTExData(quayData, arridToZdcid);
        logger.info("Fetched {} quay entries from NeTEx", quayData.size());

        rewriteZip(inputZipPath, outputZipPath, quayData, arridToZdcid);
        logger.info("Enriched GTFS written to {}", outputZipPath);

        logger.info("Adding elevator pathways to enriched GTFS…");
        String tempPath = outputZipPath + ".elevtmp";
        try {
            ElevatorEnricher.addElevatorPathways(outputZipPath, tempPath);
            java.nio.file.Files.move(java.nio.file.Paths.get(tempPath),
                java.nio.file.Paths.get(outputZipPath),
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            logger.info("Elevator pathways added successfully.");
        } catch (Exception e) {
            logger.warn("Elevator pathway enrichment failed (non-critical): {}", e.getMessage());
            java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(tempPath));
        }

        logger.info("Adding fares to enriched GTFS…");
        String fareTempPath = outputZipPath + ".faretmp";
        try {
            FareEnricher.addFares(outputZipPath, fareTempPath);
            java.nio.file.Files.move(java.nio.file.Paths.get(fareTempPath),
                java.nio.file.Paths.get(outputZipPath),
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            logger.info("Fares added successfully.");
        } catch (Exception e) {
            logger.warn("Fare enrichment failed (non-critical): {}", e.getMessage());
            java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(fareTempPath));
        }
    }

    // -------------------------------------------------------------------------
    // NeTEx data fetching
    // -------------------------------------------------------------------------

    static void fetchNeTExData(Map<String, QuayData> quayMap, Map<String, String> arridToZdcid)
            throws IOException {
        String apiKey = dotenv.get("API_KEY", null);
        URL url = URI.create(NETEX_URL).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        if (apiKey != null) conn.setRequestProperty("apiKey", apiKey);
        conn.setConnectTimeout(30_000);
        conn.setReadTimeout(300_000);

        try {
            InputStream raw = conn.getInputStream();
            String encoding = conn.getContentEncoding();
            InputStream is = (encoding != null && encoding.equalsIgnoreCase("gzip"))
                    ? new java.util.zip.GZIPInputStream(raw)
                    : raw;
            try {
                SAXParserFactory factory = SAXParserFactory.newInstance();
                factory.setNamespaceAware(true);
                SAXParser saxParser = factory.newSAXParser();
                NeTExSaxHandler handler = new NeTExSaxHandler();
                saxParser.parse(is, handler);
                quayMap.putAll(handler.quayMap);
                arridToZdcid.putAll(handler.arridToZdcid);
            } catch (SAXException | ParserConfigurationException e) {
                throw new IOException("Failed to parse NeTEx XML", e);
            }
        } finally {
            conn.disconnect();
        }
    }

    // -------------------------------------------------------------------------
    // SAX handler for NeTEx Quay elements
    // -------------------------------------------------------------------------

    static class NeTExSaxHandler extends DefaultHandler {

        final Map<String, QuayData> quayMap = new LinkedHashMap<>();
        final Map<String, String> arridToZdcid = new HashMap<>();

        private boolean inQuay = false;
        private int quayDepth = 0;
        private int depth = 0;

        private String currentQuayId;
        private String currentName;
        private String currentPublicCode;
        private String currentGmlPos;
        private String currentTariffZone;
        private String currentParentZoneRef;

        private StringBuilder charBuffer = new StringBuilder();

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attrs) {
            depth++;

            if (NS.equals(uri) && "Quay".equals(localName)) {
                inQuay = true;
                quayDepth = depth;
                currentQuayId = extractNumericId(attrs.getValue("id"));
                currentName = null;
                currentPublicCode = null;
                currentGmlPos = null;
                currentTariffZone = null;
                currentParentZoneRef = null;
            }

            if (inQuay) {
                if (NS.equals(uri) && "TariffZoneRef".equals(localName) && currentTariffZone == null) {
                    currentTariffZone = extractTariffZoneNum(attrs.getValue("ref"));
                }
                if (NS.equals(uri) && "ParentZoneRef".equals(localName)) {
                    currentParentZoneRef = extractNumericId(attrs.getValue("ref"));
                }
            }

            charBuffer = new StringBuilder();
        }

        @Override
        public void characters(char[] ch, int start, int length) {
            charBuffer.append(ch, start, length);
        }

        @Override
        public void endElement(String uri, String localName, String qName) {
            if (inQuay) {
                String text = charBuffer.toString().trim();

                if (NS.equals(uri)) {
                    if ("Name".equals(localName) && depth == quayDepth + 1 && currentName == null) {
                        currentName = text;
                    }
                    if ("PublicCode".equals(localName) && depth == quayDepth + 1) {
                        currentPublicCode = text;
                    }
                }
                if (GML_NS.equals(uri) && "pos".equals(localName) && currentGmlPos == null) {
                    currentGmlPos = text;
                }

                if (NS.equals(uri) && "Quay".equals(localName) && depth == quayDepth) {
                    storeCurrentQuay();
                    inQuay = false;
                    currentQuayId = null;
                }
            }

            depth--;
            charBuffer = new StringBuilder();
        }

        private void storeCurrentQuay() {
            if (currentQuayId == null || currentQuayId.isEmpty()) return;

            String lat = "", lon = "";
            if (currentGmlPos != null) {
                String[] parts = currentGmlPos.trim().split("\\s+");
                if (parts.length >= 2) {
                    try {
                        double x = Double.parseDouble(parts[0]);
                        double y = Double.parseDouble(parts[1]);
                        double[] wgs84 = lambert93ToWGS84(x, y);
                        lat = String.format(java.util.Locale.US, "%.7f", wgs84[0]);
                        lon = String.format(java.util.Locale.US, "%.7f", wgs84[1]);
                    } catch (NumberFormatException ignored) {}
                }
            }

            String zdcid = currentParentZoneRef != null ? currentParentZoneRef : "";
            QuayData qd = new QuayData(
                currentName != null ? currentName : "",
                lat, lon,
                currentTariffZone != null ? currentTariffZone : "",
                zdcid,
                currentPublicCode != null ? currentPublicCode : ""
            );
            quayMap.put(currentQuayId, qd);
            if (!zdcid.isEmpty()) arridToZdcid.put(currentQuayId, zdcid);
        }
    }

    // -------------------------------------------------------------------------
    // Coordinate conversion: Lambert 93 (EPSG:2154) → WGS84
    // -------------------------------------------------------------------------

    /**
     * Converts Lambert 93 (EPSG:2154) projected coordinates to WGS84
     * geographic coordinates using the inverse Lambert Conformal Conic
     * projection with GRS80 ellipsoid parameters.
     *
     * @param x easting in metres
     * @param y northing in metres
     * @return double[]{latitude, longitude} in decimal degrees
     */
    static double[] lambert93ToWGS84(double x, double y) {
        final double a = 6378137.0;
        final double f = 1.0 / 298.257222101;
        final double e = Math.sqrt(2 * f - f * f);

        final double phi1 = Math.toRadians(44.0);
        final double phi2 = Math.toRadians(49.0);
        final double phi0 = Math.toRadians(46.5);
        final double lambda0 = Math.toRadians(3.0);
        final double FE = 700000.0;
        final double FN = 6600000.0;

        double m1 = lccM(phi1, e);
        double m2 = lccM(phi2, e);
        double t1 = lccT(phi1, e);
        double t2 = lccT(phi2, e);
        double t0 = lccT(phi0, e);

        double n = (Math.log(m1) - Math.log(m2)) / (Math.log(t1) - Math.log(t2));
        double bigF = m1 / (n * Math.pow(t1, n));
        double rho0 = a * bigF * Math.pow(t0, n);

        double xAdj = x - FE;
        double yAdj = rho0 - (y - FN);
        double rho = Math.copySign(Math.sqrt(xAdj * xAdj + yAdj * yAdj), n);
        double theta = Math.atan2(xAdj, yAdj);
        double tPrime = Math.pow(rho / (a * bigF), 1.0 / n);

        double phi = Math.PI / 2 - 2 * Math.atan(tPrime);
        for (int i = 0; i < 10; i++) {
            double sinPhi = Math.sin(phi);
            phi = Math.PI / 2 - 2 * Math.atan(
                tPrime * Math.pow((1 - e * sinPhi) / (1 + e * sinPhi), e / 2));
        }

        return new double[]{Math.toDegrees(phi), Math.toDegrees(theta / n + lambda0)};
    }

    private static double lccM(double phi, double e) {
        double sinPhi = Math.sin(phi);
        return Math.cos(phi) / Math.sqrt(1 - e * e * sinPhi * sinPhi);
    }

    private static double lccT(double phi, double e) {
        double sinPhi = Math.sin(phi);
        return Math.tan(Math.PI / 4 - phi / 2) /
               Math.pow((1 - e * sinPhi) / (1 + e * sinPhi), e / 2);
    }

    // -------------------------------------------------------------------------
    // ID extraction helpers
    // -------------------------------------------------------------------------

    static String extractNumericId(String id) {
        if (id == null) return "";
        Matcher m = NETEX_ID.matcher(id);
        return m.find() ? m.group(1) : "";
    }

    static String extractTariffZoneNum(String ref) {
        if (ref == null) return "";
        Matcher m = TARIFF_ZONE_NUM.matcher(ref);
        return m.find() ? m.group(1) : "";
    }

    // -------------------------------------------------------------------------
    // Platform code resolution
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if {@code code} looks like a station name rather
     * than a platform code.
     */
    static boolean isStationName(String code) {
        if (code == null || code.isEmpty() || "-".equals(code)) return true;
        if (code.contains(" ") || code.contains("/")) return true;
        return STATION_NAME_LETTERS.matcher(code).find();
    }

    /**
     * Resolves the platform code for a NeTEx Quay.
     *
     * <p>Uses {@code PublicCode} when it is a valid platform code; otherwise
     * falls back to extracting a short code from the quay name via patterns
     * like "voie 2" or "Quai H" at the end of the name.
     */
    static String resolvePlatformCode(QuayData quay) {
        if (!isStationName(quay.publicCode())) return quay.publicCode();
        Matcher m = ARTNAME_PLATFORM.matcher(quay.name());
        return m.find() ? m.group(1) : "";
    }

    // -------------------------------------------------------------------------
    // ZIP rewriting
    // -------------------------------------------------------------------------

    private static void rewriteZip(
            String inputZipPath, String outputZipPath,
            Map<String, QuayData> quayData,
            Map<String, String> arridToZdcid) throws IOException {

        Path inputPath = Paths.get(inputZipPath);
        Path outputPath = Paths.get(outputZipPath);

        try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(inputPath));
             ZipOutputStream zout = new ZipOutputStream(Files.newOutputStream(outputPath))) {

            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                zout.putNextEntry(new ZipEntry(entry.getName()));

                if ("stops.txt".equals(entry.getName())) {
                    byte[] enriched = enrichStopsTxt(zin, quayData, arridToZdcid);
                    zout.write(enriched);
                } else {
                    zin.transferTo(zout);
                }

                zout.closeEntry();
                zin.closeEntry();
            }
        }
    }

    private static byte[] enrichStopsTxt(
            InputStream inputStream,
            Map<String, QuayData> quayData,
            Map<String, String> arridToZdcid) throws IOException {

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();

        String headerLine = reader.readLine();
        if (headerLine == null) return new byte[0];

        String[] headers = parseCsvLine(headerLine);
        int stopIdIdx = -1, platformCodeIdx = -1, parentStationIdx = -1;
        for (int i = 0; i < headers.length; i++) {
            String h = headers[i].trim();
            if ("stop_id".equals(h)) stopIdIdx = i;
            if ("platform_code".equals(h)) platformCodeIdx = i;
            if ("parent_station".equals(h)) parentStationIdx = i;
        }

        boolean columnExists = platformCodeIdx >= 0;
        String[] normalizedHeaders = Arrays.copyOf(headers, headers.length);
        for (int i = 0; i < normalizedHeaders.length; i++) {
            normalizedHeaders[i] = normalizedHeaders[i].trim();
        }

        if (columnExists) {
            sb.append(headerLine).append("\n");
        } else {
            sb.append(headerLine).append(",platform_code\n");
            normalizedHeaders = Arrays.copyOf(normalizedHeaders, normalizedHeaders.length + 1);
            normalizedHeaders[normalizedHeaders.length - 1] = "platform_code";
            platformCodeIdx = normalizedHeaders.length - 1;
        }

        if (stopIdIdx < 0) {
            String line;
            while ((line = reader.readLine()) != null) sb.append(line).append("\n");
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        }

        Set<String> existingArrids = new HashSet<>();
        // "IDFM:monomodalStopPlace:{zdcid}" zdcid -> that stop's own parent_station. Every such
        // stop is itself only location_type=0 (a "logical"/representative point for the physical
        // complex) sitting one level below the true location_type=1 GTFS Station -- so a newly
        // created quay must be parented to THAT station, not to the monomodalStopPlace stop
        // itself, to be a proper sibling other tooling finds by walking direct station children
        // (and to satisfy the GTFS rule that a location_type=0 stop's parent must be a Station).
        Map<String, String> monomodalStopPlaceParent = new HashMap<>();
        int enrichedCount = 0;
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) continue;

            String[] fields = parseCsvLine(line);
            String stopId = stopIdIdx < fields.length ? fields[stopIdIdx].trim() : "";

            if (parentStationIdx >= 0 && parentStationIdx < fields.length
                    && stopId.startsWith("IDFM:monomodalStopPlace:")) {
                String zdcid = stopId.substring("IDFM:monomodalStopPlace:".length());
                String parent = fields[parentStationIdx].trim();
                if (!parent.isEmpty()) {
                    monomodalStopPlaceParent.put(zdcid, parent);
                }
            }

            Matcher m = PURE_NUMERIC_STOP_ID.matcher(stopId);
            if (m.matches()) {
                String arrid = m.group(1);
                existingArrids.add(arrid);

                QuayData qd = quayData.get(arrid);
                String platformCode = (qd != null) ? resolvePlatformCode(qd) : "";

                if (!platformCode.isEmpty() && platformCodeIdx < fields.length) {
                    fields[platformCodeIdx] = platformCode;
                    sb.append(joinCsvLine(fields)).append("\n");
                    enrichedCount++;
                    continue;
                }
            }
            sb.append(line).append("\n");
        }

        int createdCount = 0;
        for (Map.Entry<String, QuayData> e : quayData.entrySet()) {
            String arrid = e.getKey();
            if (existingArrids.contains(arrid)) continue;

            String zdcid = arridToZdcid.get(arrid);
            if (zdcid == null || zdcid.isBlank()) continue;

            QuayData qd = e.getValue();
            if (qd.lat().isEmpty() || qd.lon().isEmpty()) continue;

            // zdcid is the Quay's ParentZoneRef (a monomodalStopPlace id) with just the numeric
            // suffix extracted. Prefer that monomodalStopPlace stop's own parent_station (the
            // true location_type=1 Station) so the new quay lands as its sibling; fall back to
            // the monomodalStopPlace id itself only if that lookup is unavailable (e.g. it wasn't
            // present in this particular stops.txt) rather than a bare "IDFM:{zdcid}", which
            // matches no real stop at all and breaks the station→platform hierarchy for any
            // consumer that relies on it (GTFS-RT still worked either way because trip matching
            // resolves parent stations by their child quays directly, bypassing this field).
            String parentStation = monomodalStopPlaceParent.getOrDefault(
                    zdcid, "IDFM:monomodalStopPlace:" + zdcid);
            String newRow = buildNewStopRow(
                    normalizedHeaders, platformCodeIdx,
                    "IDFM:" + arrid, qd.name(), qd.lat(), qd.lon(),
                    qd.tariffZone(), parentStation, resolvePlatformCode(qd));
            sb.append(newRow).append("\n");
            createdCount++;
        }

        logger.info("stops.txt: enriched {} stop(s), created {} missing stop(s)",
                enrichedCount, createdCount);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String buildNewStopRow(
            String[] headers, int platformCodeIdx,
            String stopId, String stopName,
            String stopLat, String stopLon,
            String zoneId, String parentStation,
            String platformCode) {

        String[] fields = new String[headers.length];
        Arrays.fill(fields, "");

        for (int i = 0; i < headers.length; i++) {
            switch (headers[i]) {
                case "stop_id"            -> fields[i] = stopId;
                case "stop_name"          -> fields[i] = stopName;
                case "stop_lat"           -> fields[i] = stopLat;
                case "stop_lon"           -> fields[i] = stopLon;
                case "zone_id"            -> fields[i] = zoneId;
                case "location_type"      -> fields[i] = "0";
                case "parent_station"     -> fields[i] = parentStation;
                case "stop_timezone"      -> fields[i] = "Europe/Paris";
                case "wheelchair_boarding"-> fields[i] = "0";
                case "platform_code"      -> fields[i] = platformCode;
                default                   -> {}
            }
        }
        return joinCsvLine(fields);
    }

    // -------------------------------------------------------------------------
    // Minimal CSV helpers (also used by ElevatorEnricher, FareEnricher, GTFSFetcher)
    // -------------------------------------------------------------------------

    static String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields.toArray(new String[0]);
    }

    static String joinCsvLine(String[] fields) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append(',');
            String f = fields[i];
            if (f.contains(",") || f.contains("\"") || f.contains("\n")) {
                sb.append('"').append(f.replace("\"", "\"\"")).append('"');
            } else {
                sb.append(f);
            }
        }
        return sb.toString();
    }
}
