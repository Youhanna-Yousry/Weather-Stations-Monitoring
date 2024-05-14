package dao;

import dto.StationStatusMsgDTO;
import dto.WeatherDTO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ParquetWriterWithAvro {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetWriterWithAvro.class);

    private static final Schema SCHEMA;
    private static final Schema INNER_SCHEMA;
    private static final String SCHEMA_LOCATION = "src/main/resources/avroSchema.avsc";
    private static final String INNER_SCHEMA_LOCATION = "src/main/resources/innerAvroSchema.avsc";
    private static final String OUT_PATH = "src/main/resources/archive/";
    private static final int BATCH_SIZE = 100; // Batch size of 10K records
    private List<GenericData.Record> buffer = new ArrayList<>();
    public Map<Long, Map<String,List<GenericData.Record>>> buffers;

    static {
        try (InputStream inputStream = new FileInputStream(SCHEMA_LOCATION);
             InputStream inputStreamInner = new FileInputStream(INNER_SCHEMA_LOCATION))
        {
            SCHEMA = new Schema.Parser().parse(IOUtils.toString(inputStream, StandardCharsets.UTF_8));
            INNER_SCHEMA = new Schema.Parser().parse(IOUtils.toString(inputStreamInner, StandardCharsets.UTF_8));
            System.out.println("Schema: "+SCHEMA.toString());
        } catch (IOException e) {
            LOGGER.error("Can't read SCHEMA file from {}", SCHEMA_LOCATION);
            throw new RuntimeException("Can't read SCHEMA file from" + SCHEMA_LOCATION, e);
        }
    }


    public static void main(String[] args) {
        ParquetWriterWithAvro writer = new ParquetWriterWithAvro();
        /* Testing writing in Batches */
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> {
            try {
                List<GenericData.Record> records = generateRecords(10); // Generate records proportional to the batch size per second
                writer.appendToParquet(records);
            } catch (IOException | NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }, 0, 1, TimeUnit.SECONDS); // Generate records every second

        // Sleep for a while to simulate program running
        try {
            Thread.sleep(11000); // Simulate running for 60 seconds
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executorService.shutdown();
    }

    private static List<GenericData.Record> generateRecords(int count) throws NoSuchFieldException, IllegalAccessException {
        List<GenericData.Record> records = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            StationStatusMsgDTO record = new StationStatusMsgDTO(random.nextLong(), random.nextLong(), "LOW", random.nextLong(),
                    new WeatherDTO(random.nextInt(0, 100), random.nextInt(0,100), random.nextInt(0,100)));
            records.add(createRecord(record));
        }
        return records;
    }

    public static GenericData.Record createRecord(StationStatusMsgDTO dto) throws IllegalAccessException {
        GenericData.Record record = new GenericData.Record(SCHEMA);
        Class<?> dtoClass = dto.getClass();
        Field[] fields = dtoClass.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            Object value = field.get(dto);
            if (field.getName().equals("weather")) { // Handle the nested object
                GenericData.Record weatherRecord = new GenericData.Record(INNER_SCHEMA);
                WeatherDTO weatherDTO = (WeatherDTO) value;
                weatherRecord.put("humidity", weatherDTO.getHumidity());
                weatherRecord.put("temperature", weatherDTO.getTemperature());
                weatherRecord.put("windSpeed", weatherDTO.getWindSpeed());
                record.put("weather", weatherRecord);
            } else {
                record.put(field.getName(), value);
            }
        }
        return record;
    }

    public void appendToParquet(List<GenericData.Record> recordsToWrite) throws IOException {
        buffer.addAll(recordsToWrite);
        if (buffer.size() >= BATCH_SIZE) {
            writeToParquet(buffer);
            buffer.clear();
        }
    }

//    @SuppressWarnings("unchecked")
    private void writeToParquet(List<GenericData.Record> recordsToWrite) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        ParquetWriter<GenericData.Record> writer;
        if (!fs.exists(new Path(OUT_PATH + "station12345/output2.parquet"))) {
            writer = AvroParquetWriter
                    .<GenericData.Record>builder(new Path(OUT_PATH + "station12345/output2.parquet"))
                    .withSchema(SCHEMA)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.GZIP)
                    .withPageSize(1024)
                    .withWriteMode(ParquetFileWriter.Mode.CREATE)
                    .build();
        } else {
            writer = AvroParquetWriter
                    .<GenericData.Record>builder(new Path(OUT_PATH + "station12345/output2.parquet"))
                    .withSchema(SCHEMA)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.GZIP)
                    .withPageSize(1024)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build();
        }

        try {
            int writtenCount = 0;
            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
                writtenCount++;
            }
            System.out.println("Written " + writtenCount + " records to " + OUT_PATH + "..");
        } finally {
            writer.close();
        }
    }
}
