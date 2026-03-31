package org.example.datapipeline.executor.io;

import java.util.Map;
import java.util.HashMap;

public class DataIORegistry {
    private static final Map<String, DataReader> readerRegistry = new HashMap<>();
    private static final Map<String, DataWriter> writerRegistry = new HashMap<>();

    static {
        registerReader(new CsvDataReader());
        registerWriter(new CsvDataWriter());
    }

    public static void registerReader(DataReader reader) { 
        readerRegistry.put(reader.getType().toLowerCase(), reader); 
    }
    
    public static void registerWriter(DataWriter writer) { 
        writerRegistry.put(writer.getType().toLowerCase(), writer); 
    }

    public static DataReader getReader(String type) {
        DataReader r = readerRegistry.get(type.toLowerCase());
        if (r == null) throw new RuntimeException("Unsupported input type: " + type);
        return r;
    }

    public static DataWriter getWriter(String type) {
        DataWriter w = writerRegistry.get(type.toLowerCase());
        if (w == null) throw new RuntimeException("Unsupported output type: " + type);
        return w;
    }
}
