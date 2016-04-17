package com.evolvedbinary.rocksdb.wbwi.performance;

import org.rocksdb.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by aretter on 29/03/2016.
 */
public class PerformanceTest {

    //private final static NumberFormat nf = NumberFormat.getI

    public final static void main(final String args[]) throws IOException, RocksDBException {

        final Path dataDir = Paths.get(args[0]);
        final Path dbDir = Paths.get(args[1]);

        //load the serialized write batch data from disk
        final Map<String, List<Record>> cfRecords = loadWbFromDisk(dataDir);

        final long count = cfRecords.values().stream().mapToLong(Collection::size).sum();
        System.out.println();
        System.out.println("Benchmarking with: " + count + " key/value pairs");


        final long directTime = withDb(dbDir, cfRecords.keySet(), (rocksDb, namedHandles) -> benchmark(rocksDb, namedHandles, (rocksDb2, namedHandles2) -> {
            try {
                directPut(rocksDb2, namedHandles2, cfRecords);
            } catch (final RocksDBException e) {
                throw new RuntimeException(e);
            }
        }));
        System.out.println("Direct puts took: " + directTime + "ms");
        System.out.println();

        final long writeBatchTime = withDb(dbDir, cfRecords.keySet(), (rocksDb, namedHandles) -> benchmark(rocksDb, namedHandles, (rocksDb2, namedHandles2) -> {
            try {
                writeBatchPut(rocksDb2, namedHandles2, cfRecords);
            } catch (final RocksDBException e) {
                throw new RuntimeException(e);
            }
        }));
        System.out.println("WriteBatch puts took: " + writeBatchTime + "ms");
        System.out.println();

        final long writeBatchWithIndexTime = withDb(dbDir, cfRecords.keySet(), (rocksDb, namedHandles) -> benchmark(rocksDb, namedHandles, (rocksDb2, namedHandles2) -> {
            try {
                writeBatchWithIndexPut(rocksDb2, namedHandles2, cfRecords);
            } catch (final RocksDBException e) {
                throw new RuntimeException(e);
            }
        }));
        System.out.println("WriteBatchWithIndex puts took: " + writeBatchWithIndexTime + "ms");
        System.out.println();
    }

    private static long benchmark(final RocksDB rocksDb, final Map<String, ColumnFamilyHandle> namedHandles, final BiConsumer<RocksDB, Map<String, ColumnFamilyHandle>> consumer) {
        final long start = System.currentTimeMillis();
        consumer.accept(rocksDb, namedHandles);
        return System.currentTimeMillis() - start;
    }

    private static void directPut(final RocksDB rocksDb, final Map<String, ColumnFamilyHandle> namedHandles, final Map<String, List<Record>> cfRecords) throws RocksDBException {
        for(final Map.Entry<String, List<Record>> records : cfRecords.entrySet()) {
            final ColumnFamilyHandle cfHandle = namedHandles.get(records.getKey());
            for(final Record record : records.getValue()) {

                switch(record.type) {
                    case PUT:
                        rocksDb.put(cfHandle, record.key, record.value);
                        break;

                    case MERGE:
                        rocksDb.merge(cfHandle, record.key, record.value);
                        break;

                    case DELETE:
                        rocksDb.remove(cfHandle, record.key);
                        break;

                    case LOG:
                        throw new UnsupportedOperationException("LOG is not yet supported");
                }
            }
        }
    }

    private static void writeBatchPut(final RocksDB rocksDb, final Map<String, ColumnFamilyHandle> namedHandles, final Map<String, List<Record>> cfRecords) throws RocksDBException {
        final WriteBatch wb = new WriteBatch();

        for(final Map.Entry<String, List<Record>> records : cfRecords.entrySet()) {
            final ColumnFamilyHandle cfHandle = namedHandles.get(records.getKey());
            for(final Record record : records.getValue()) {

                switch(record.type) {
                    case PUT:
                        wb.put(cfHandle, record.key, record.value);
                        break;

                    case MERGE:
                        wb.merge(cfHandle, record.key, record.value);
                        break;

                    case DELETE:
                        wb.remove(cfHandle, record.key);
                        break;

                    case LOG:
                        throw new UnsupportedOperationException("LOG is not yet supported");
                }
            }
        }

        System.out.println(wb.count() + " entries in the WriteBatch");
        final long start = System.currentTimeMillis();
        rocksDb.write(new WriteOptions(), wb);
        System.out.println("Writing the WriteBatch took: " + (System.currentTimeMillis() - start) + "ms");
    }

    private static void writeBatchWithIndexPut(final RocksDB rocksDb, final Map<String, ColumnFamilyHandle> namedHandles, final Map<String, List<Record>> cfRecords) throws RocksDBException {
        final WriteBatchWithIndex wbwi = new WriteBatchWithIndex(true);

        for(final Map.Entry<String, List<Record>> records : cfRecords.entrySet()) {
            final ColumnFamilyHandle cfHandle = namedHandles.get(records.getKey());
            for(final Record record : records.getValue()) {

                switch(record.type) {
                    case PUT:
                        wbwi.put(cfHandle, record.key, record.value);
                        break;

                    case MERGE:
                        wbwi.merge(cfHandle, record.key, record.value);
                        break;

                    case DELETE:
                        wbwi.remove(cfHandle, record.key);
                        break;

                    case LOG:
                        throw new UnsupportedOperationException("LOG is not yet supported");
                }
            }
        }

        System.out.println(wbwi.count() + " entries in the WriteBatchWithIndex");
        final long start = System.currentTimeMillis();
        rocksDb.write(new WriteOptions(), wbwi);
        System.out.println("Writing the WriteBatchWithIndex took: " + (System.currentTimeMillis() - start) + "ms");
    }

    private static <T> T withDb(final Path dbDir, final Collection<String> cfNames, final BiFunction<RocksDB, Map<String, ColumnFamilyHandle>, T> databaseOp) throws RocksDBException {
        //open the database
        DBOptions options = null;
        RocksDB rocksDb = null;
        List<ColumnFamilyDescriptor> cfDescriptors = null;
        AbstractComparator domStoreComparator = null;
        List<ColumnFamilyHandle> cfHandles = null;
        try {
            options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);

            cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions().optimizeLevelStyleCompaction()));

            for(final String cfName : cfNames) {
                final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                        .optimizeLevelStyleCompaction();
                if(cfName.equals("XML_DOM_STORE")) {
                    domStoreComparator = new NativeDomKeyComparator();
                    cfOptions.setComparator(domStoreComparator);
                }
                cfDescriptors.add(new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), cfOptions));
            }

            cfHandles = new ArrayList<>();
            rocksDb = RocksDB.open(options, dbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);

            final Map<String, ColumnFamilyHandle> namedHandles = new HashMap<>();
            for(int i = 0; i < cfHandles.size(); i++) {
                namedHandles.put(new String(cfDescriptors.get(i).columnFamilyName(), UTF_8), cfHandles.get(i));
            }

            return databaseOp.apply(rocksDb, namedHandles);

        } finally {
            if(cfHandles != null) {
                cfHandles.forEach(ColumnFamilyHandle::dispose);
            }
            if(cfDescriptors != null) {
                cfDescriptors.forEach(
                        cfDescriptor -> cfDescriptor.columnFamilyOptions().dispose());
            }
            if(rocksDb != null) {
                rocksDb.close();
            }
            options.dispose();
            domStoreComparator.dispose();

            if(rocksDb != null) {
                rocksDb.close();
            }
        }
    }

    /**
     * loads all the column-families of a serialized write batch from disk
     *
     * @return Map<ColumnFamilyName, Records>
     */
    private static Map<String, List<Record>> loadWbFromDisk(final Path dir) throws IOException {
        final Map<String, List<Record>> fileRecords = new HashMap<>();

        try(final Stream<Path> children = Files.list(dir).filter(Files::isRegularFile)) {
            for (final Path file : children.collect(Collectors.toList())) {
                System.out.println("Loading Column Family: " + file.getFileName().toString() + "...");
                final List<Record> records = loadWbCfIntoMemory(file);
                System.out.println("...Loaded " + records.size() + " key/value pairs.");
                fileRecords.put(file.getFileName().toString(), records);
            }
        }

        return fileRecords;
    }

    /**
     * Loads a serialized write-batch column family into memory
     */
    private static List<Record> loadWbCfIntoMemory(final Path file) throws IOException {
        final List<Record> records = new ArrayList<>();

        try(final SeekableByteChannel channel = Files.newByteChannel(file)) {

            //final ByteBuffer buf = ByteBuffer.allocate(1024); //TODO(AR) reinstate
            final ByteBuffer buf = ByteBuffer.allocate(1024 * 1024 * 150); //150MB

            int read = -1;
            while((read = channel.read(buf)) > 0) {
                buf.flip();

                //TODO(AR) need to consider stepping across the boundary of ByteBuffer, e.g. 1024 bytes

                Byte type = null;
                byte[] key = null;
                byte[] value = null;

                //read records from the buf
                while(buf.hasRemaining()) {

                    //type
                    if(type == null) {
                        type = buf.get();
                    }

                    //key
                    if(key == null && buf.remaining() >= 2) {
                        buf.mark();
                        final short keyLen = buf.getShort();
                        if(buf.remaining() >= keyLen) {
                            key = new byte[keyLen];
                            buf.get(key);
                        } else {
                            buf.reset();
                        }
                    }

                    //value
                    if(key != null && value == null && buf.remaining() >= 4) {
                        buf.mark();
                        final int valueLen = buf.getInt();
                        if(buf.remaining() >= valueLen) {
                            value = new byte[valueLen];
                            buf.get(value);
                        } else {
                            buf.reset();
                        }
                    }

                    if(type != null && key != null && value != null) {
                        records.add(new Record(byteType(type), key, value));
                        type = null;
                        key = null;
                        value = null;
                    } else {
                        //reposition the buffer

                        if(key != null) {
                            //rewind to re-read key in next loop
                            buf.position(buf.position() - key.length + 2);
                        }

                        if(type != null) {
                            //rewind to re-read type in next loop
                            buf.position(buf.position() - 1);
                        }

                        break;
                    }
                }

               buf.compact();
            }
        }

        return records;
    }

    private static WBWIRocksIterator.WriteType byteType(final byte type) {
        final WBWIRocksIterator.WriteType result;

        switch(type) {
            case 1:
                result = WBWIRocksIterator.WriteType.PUT;
                break;

            case 2:
                result = WBWIRocksIterator.WriteType.MERGE;
                break;

            case 4:
                result = WBWIRocksIterator.WriteType.DELETE;
                break;

            case 8:
                result = WBWIRocksIterator.WriteType.LOG;
                break;

            default:
                throw new IllegalArgumentException("Unknown case type");
        }

        return result;
    }

    private static class Record {
        final WBWIRocksIterator.WriteType type;
        final byte[] key;
        final byte[] value;

        private Record(final WBWIRocksIterator.WriteType type, final byte[] key, final byte[] value) {
            this.type = type;
            this.key = key;
            this.value = value;
        }
    }
}
