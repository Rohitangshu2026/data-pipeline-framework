package org.example.datapipeline.executor.iterator;

import org.json.JSONArray;
import org.json.JSONObject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Iterator;

public class ApiDataIterator implements DataIterator {
    private final Iterator<Object> arrayIter;
    private final String[] fields;
    private boolean headerReturned = false;

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
