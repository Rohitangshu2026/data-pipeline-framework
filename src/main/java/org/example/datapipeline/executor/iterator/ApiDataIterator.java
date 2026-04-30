package org.example.datapipeline.executor.iterator;

import org.json.JSONArray;
import org.json.JSONObject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Iterator;

/**
 * {@link DataIterator} implementation that streams rows from an HTTP JSON API response.
 *
 * <p>On construction, performs a synchronous HTTP GET to the given URL using Java 11's
 * {@link java.net.http.HttpClient} (with redirect following enabled). The response body
 * is parsed as a JSON object, and the array at the given {@code jsonPath} key is extracted.
 * Subsequent calls to {@link #next()} iterate over the parsed {@link org.json.JSONArray}
 * in memory.
 *
 * <p>The iterator emits:
 * <ol>
 *   <li>A header row containing the field names from the {@code fields} parameter
 *       (comma-separated, in the declared order).</li>
 *   <li>One data row per JSON object in the array, with each cell being
 *       {@code String.valueOf(obj.opt(fieldName))} — gracefully handling null, boolean,
 *       integer, and string field values.</li>
 * </ol>
 *
 * <p>Because the full JSON array is loaded eagerly during construction, this iterator is
 * best suited for moderate-sized API responses (up to tens of thousands of records). For
 * very large APIs, a pagination-aware or streaming JSON parser would be more appropriate.
 *
 * <p>HTTP errors (status ≥ 400) are reported as a {@link RuntimeException} during
 * construction.
 */
public class ApiDataIterator implements DataIterator {
    private final Iterator<Object> arrayIter;
    private final String[] fields;
    private boolean headerReturned = false;

    /**
     * Fetches and parses the API response, preparing the internal iterator.
     *
     * <p>Performs a synchronous HTTP GET to {@code url}. If the response status is ≥ 400,
     * throws a {@link RuntimeException} with the status code and body. Otherwise, parses
     * the body as JSON and extracts the array at the key {@code jsonPath}.
     *
     * @param url       the HTTP/HTTPS endpoint URL
     * @param jsonPath  the top-level JSON key whose value is the array to iterate
     * @param fieldsStr comma-separated field names to extract from each JSON object
     * @throws RuntimeException if the HTTP request fails, the status is ≥ 400, or the
     *                          specified JSON path does not resolve to an array
     */
    public ApiDataIterator(String url, String jsonPath, String fieldsStr) {
        try {
            HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 400) {
                throw new RuntimeException("API returned HTTP " + response.statusCode() + ": " + response.body());
            }

            JSONObject root = new JSONObject(response.body());
            JSONArray array = root.optJSONArray(jsonPath);
            if (array == null) {
                throw new RuntimeException("Could not find JSON array at path: " + jsonPath);
            }

            this.arrayIter = array.iterator();
            this.fields = fieldsStr.split(",");
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch or parse API data from " + url, e);
        }
    }

    @Override
    public boolean hasNext() {
        return !headerReturned || arrayIter.hasNext();
    }

    @Override
    public String[] next() {
        if (!headerReturned) {
            headerReturned = true;
            return fields;
        }
        JSONObject obj = (JSONObject) arrayIter.next();
        String[] row = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            row[i] = String.valueOf(obj.opt(fields[i].trim()));
        }
        return row;
    }
}
