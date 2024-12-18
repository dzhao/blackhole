package com.example;
import org.rocksdb.*;
public class RocksDBSSTReader {
    static {
        System.out.println("Loading RocksDB library...");
        try {
            System.out.println("Current working directory: " + System.getProperty("user.dir"));
            System.out.println("Attempting to load RocksDB native library...");
            
            RocksDB.loadLibrary();
            
            System.out.println("RocksDB library loaded successfully");
            System.out.flush();
        } catch (Exception e) {
            System.err.println("Failed to load RocksDB library: " + e.getMessage());
            System.err.println("Make sure the RocksDB native library is in java.library.path");
            System.err.println("Current java.library.path: " + System.getProperty("java.library.path"));
            System.err.println("Exception type: " + e.getClass().getName());
            System.err.println("Stack trace:");
            e.printStackTrace();
            System.err.flush();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        System.out.println("Program started");

        if (args.length < 1) {
            System.out.println("No arguments provided");
            System.err.println("Usage: RocksDBSSTReader <sst-file-path>");
            System.exit(1);
        }
        String sstFilePath = args[0];
        System.out.println("Reading SST file: " + sstFilePath);
        Options options = new Options();
        options.useCappedPrefixExtractor(64);
        PlainTableConfig config = new PlainTableConfig();
        // Add prefix extractor (for example, using fixed prefix length of 4)

        options.setTableFormatConfig(config);

        int count = 0;
        try (
                ReadOptions readOptions = new ReadOptions();
                SstFileReader reader = new SstFileReader(options)) {

            // Configure options for plain format

            // Open the SST file
            reader.open(sstFilePath);

            // Get an iterator for the SST file
            SstFileReaderIterator iterator = reader.newIterator(readOptions);

            // Iterate through all key-value pairs
            iterator.seekToFirst();
            count = 0;
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();

                // Print key-value pairs (assuming UTF-8 encoding)
//                System.out.printf("Key: %s, Value: %s%n",
//                    new String(key),
//                    new String(value));
                count++;
                iterator.next();
            }

        } catch (RocksDBException e) {
            System.err.println("Error reading SST file: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println(count);
    }
}
