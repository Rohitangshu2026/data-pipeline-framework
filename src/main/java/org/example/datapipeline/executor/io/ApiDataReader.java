package org.example.datapipeline.executor.io;

import org.example.datapipeline.executor.iterator.DataIterator;
import org.example.datapipeline.executor.iterator.ApiDataIterator;
import java.util.Map;

/**
 * {@link DataReader} implementation that fetches data from an HTTP JSON API endpoint.
 *
 * <p>Registered in {@link DataIORegistry} under the type key {@code "api"}. Designed for
 * pipeline stages that ingest data from REST APIs (e.g. a university course catalogue, a
 * live product feed) rather than local files.
 *
 * <p>Required parameters:
 * <ul>
 *   <li>{@code url}       – the full HTTP/HTTPS URL to fetch (GET request)</li>
 *   <li>{@code json_path} – the top-level JSON key whose value is the array to iterate
 *       (e.g. {@code "results"} for {@code {"results": [...]}})</li>
 *   <li>{@code fields}    – comma-separated list of field names to extract from each
 *       JSON object (determines column order in the output rows)</li>
 * </ul>
 *
 * <p>Delegates to {@link ApiDataIterator} which performs a synchronous HTTP GET using the
 * Java 11 {@link java.net.http.HttpClient}, parses the JSON response, and streams the
 * array elements row by row.
 */
public class ApiDataReader implements DataReader {

    /** @return {@code "api"} – the type string for this reader */
    @Override
    public String getType() { return "api"; }

    /**
     * Creates a streaming iterator over the JSON array returned by the configured API.
     *
     * <p>The HTTP GET is performed eagerly in the {@link ApiDataIterator} constructor;
     * subsequent calls to {@link DataIterator#next()} iterate over the parsed in-memory
     * array.
     *
     * @param params parameter map containing {@code url}, {@code json_path}, and
     *               {@code fields}
     * @return a new {@link ApiDataIterator} ready to stream the API results
     * @throws RuntimeException if any required parameter is missing or the HTTP request fails
     */
    @Override
    public DataIterator createIterator(Map<String, String> params) {
        String url = params.get("url");
        String jsonPath = params.get("json_path");
        String fields = params.get("fields");

        if (url == null) throw new RuntimeException("Missing 'url' parameter for API input");
        if (jsonPath == null) throw new RuntimeException("Missing 'json_path' parameter for API input");
        if (fields == null) throw new RuntimeException("Missing 'fields' parameter for API input (comma separated columns to extract)");

        return new ApiDataIterator(url, jsonPath, fields);
    }
}
