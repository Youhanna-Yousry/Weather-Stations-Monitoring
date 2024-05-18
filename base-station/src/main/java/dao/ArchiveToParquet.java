package dao;

import dto.StationStatusMsgDTO;
import dto.WeatherDTO;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ArchiveToParquet {
    private static final Schema STATUS_SCHEMA;
    private static final Schema WEATHER_SCHEMA;
    private static final String STATUS_SCHEMA_LOCATION = "src/main/resources/archiving_files/avroSchema.avsc";
    private static final String WEATHER_SCHEMA_LOCATION= "src/main/resources/archiving_files/innerAvroSchema.avsc";
    private static final String ARCHIVE_DIRECTORY = "src/main/resources/archiving_files/archive/";
    private static final int BATCH_SIZE = 10_000; // batch size is 10k, but when testing small functionalities: we may need to change this value.
    private Map<Long, Map<String, List<GenericData.Record>>> buffers;
    private int buffersSize;

    static {
        try(InputStream statusStream = new FileInputStream(STATUS_SCHEMA_LOCATION);
            InputStream weatherStream= new FileInputStream(WEATHER_SCHEMA_LOCATION))
        {
            STATUS_SCHEMA = new Schema.Parser().parse(IOUtils.toString(statusStream, StandardCharsets.UTF_8));
            WEATHER_SCHEMA = new Schema.Parser().parse(IOUtils.toString(weatherStream, StandardCharsets.UTF_8));
            System.out.println("Schema: " + STATUS_SCHEMA.toString());
        } catch (IOException e) {
            final Logger LOGGER = LoggerFactory.getLogger(ArchiveToParquet.class);
            LOGGER.error("Can't read SCHEMA file from {}", STATUS_SCHEMA_LOCATION);
            throw new RuntimeException(e);
        }
    }

    public ArchiveToParquet() {
        buffers = new HashMap<>();
        buffersSize = 0;
    }

    public void writeToParquet(StationStatusMsgDTO stationStatusMsgDTO) {
        try {
            Long stationId = stationStatusMsgDTO.getStationId();
            checkStationId(stationId);
            Long timestamp = stationStatusMsgDTO.getStatusTimestamp();
            String date = checkDate(timestamp, stationId);

            GenericData.Record record = createRecord(stationStatusMsgDTO);
            writeRecord(record, stationId, date);
        } catch (Exception e) {
            System.out.println("Couldn't write to parquet!!\n" + e);
        }
    }

    /* Extracting StationId */
    private void checkStationId(Long stationId) {
        if(!buffers.containsKey(stationId))
            buffers.put(stationId, new HashMap<>());
    }

    /* Extracting Date */
    private String checkDate(Long time, Long stationId) {
        LocalDate localDate = new org.joda.time.LocalDate(new Date(time));
        String date =
                localDate.getDayOfMonth() + "-"
                + localDate.getMonthOfYear() + "-"
                + localDate.getYear();

        if(!buffers.get(stationId).containsKey(date)) {
            buffers.get(stationId).put(date, new ArrayList<>());
        }

        return date;
    }

    /* Generate Record */
    private GenericData.Record createRecord(StationStatusMsgDTO dto) throws IllegalAccessException {
        GenericData.Record record = new GenericData.Record(STATUS_SCHEMA);
        Class<?> dtoClass = dto.getClass();
        Field[] fields = dtoClass.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            Object value = field.get(dto);
            if (field.getName().equals("weather")) { // Handle the nested object
                GenericData.Record weatherRecord = new GenericData.Record(WEATHER_SCHEMA);
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

    /* Add Record */
//    private void writeRecord(GenericData.Record record, Long stationId, String date) throws IOException {
//        List<GenericData.Record> buffer = buffers.get(stationId).get(date);
//        buffer.add(record);
//        if(buffer.size() >= BATCH_SIZE) {
//            String fileId = UUID.randomUUID().toString();
//            String fileToWritePath = ARCHIVE_DIRECTORY +
//                    "station_" + stationId + "/" +
//                    "day_" + date + "/" + fileId + ".parquet";
//            /* Write Batch */
//            writeFile(buffer, fileToWritePath);
//            /* Flush the Record */
//            buffer.clear();
//        }
//    }
    private void writeRecord(GenericData.Record record, Long stationId, String date) throws IOException {
        buffers.get(stationId).get(date).add(record);
        buffersSize++;
        if(buffersSize >= BATCH_SIZE) {
            writeBatchAndReset();
        }
    }

    private void writeBatchAndReset() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        for(Map.Entry<Long, Map<String, List<GenericData.Record>>> entry : buffers.entrySet()) {
            for(Map.Entry<String, List<GenericData.Record>> innerEntry : entry.getValue().entrySet()) {
                String fileId = UUID.randomUUID().toString();
                String path = ARCHIVE_DIRECTORY +
                        "station_" + entry.getKey() + "/" +
                        "day_" + innerEntry.getKey() + "/" + fileId + ".parquet";

                ParquetWriter<GenericData.Record> writer;
                writer = AvroParquetWriter
                        .<GenericData.Record>builder(new Path(path))
                        .withSchema(STATUS_SCHEMA)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.GZIP)
                        .withWriteMode(ParquetFileWriter.Mode.CREATE)
                        .build();
                try {
                    int written = 0;
                    for(GenericData.Record record : innerEntry.getValue()) {
                        writer.write(record);
                        written++;
                    }
                    System.out.println("station_" + entry.getKey() + "_date_" + innerEntry.getKey() + " ==> " + written + " records");
                } finally {
                    writer.close();
                }
            }
        }
        buffers = new HashMap<>();
        buffersSize = 0;
    }

//    private void writeFile(List<GenericData.Record> buffer, String path) throws IOException {
//        FileSystem fs = FileSystem.get(new Configuration());
//        ParquetWriter<GenericData.Record> writer;
//        writer = AvroParquetWriter
//                .<GenericData.Record>builder(new Path(path))
//                .withSchema(STATUS_SCHEMA)
//                .withConf(new Configuration())
//                .withCompressionCodec(CompressionCodecName.GZIP)
//                .withWriteMode(ParquetFileWriter.Mode.CREATE)
//                .build();
//        try {
//            for(GenericData.Record record : buffer) {
//                writer.write(record);
//            }
//        } finally {
//            writer.close();
//        }
//    }

    /*************** TESTING *****************/
    static int totalRecords = 0;
    private int printRemainingItems() {
        int totalSum = 0;
        for(Map.Entry<Long, Map<String, List<GenericData.Record>>> entry : buffers.entrySet()) {
            System.out.print("Station_" + entry.getKey() + " --> ");
            int sum = 0;
            for(Map.Entry<String, List<GenericData.Record>> inner : entry.getValue().entrySet()) {
                sum += inner.getValue().size();
            }
            System.out.println(sum);
            totalSum += sum;
        }
        return totalSum;
    }
    static long sequenceNum = 1000000;
    private static List<StationStatusMsgDTO> generateRecords(int count, long[] stations, long[] dates, String[] statuses) throws NoSuchFieldException, IllegalAccessException {
        List<StationStatusMsgDTO> objects = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < count; i++) {
            int stationIdx = random.nextInt(0,stations.length);
            int dateIdx = random.nextInt(0,dates.length);
            int statusIdx = random.nextInt(0,statuses.length);
            float prob = random.nextFloat(1);
            String status;
            if(prob < 0.3)      status = statuses[0];
            else if(prob < 0.7) status = statuses[1];
            else                status = statuses[2];
                StationStatusMsgDTO record = new StationStatusMsgDTO(stations[stationIdx], sequenceNum++, status, dates[dateIdx],
                    new WeatherDTO(random.nextInt(0, 100), random.nextInt(0,100), random.nextInt(0,100)));
            objects.add(record);
            totalRecords++;
        }
        return objects;
    }

    public static void main(String[] args) {
        long[] stations = new long[] {11L, 12L, 13L, 14L, 15L, 16, 17L, 18L, 19L, 20L};
        long[] dates = new long[] {1715579709000L, 1715678151279L}; // 13/5, 14/5
//               , 1715783639000L , 1715870039000L, 1715956439000L, 1716042839000L}; // 13/5 , 14/5 , 15/5 , 16/5 , 17/5 , 18/5
        String[] statuses = new String[] {"LOW", "MEDIUM", "HIGH"};
        ArchiveToParquet writer = new ArchiveToParquet();
        /* Testing writing in Batches */
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> {
            try {
                for (int i=0; i<4; i++) {
                    List<StationStatusMsgDTO> records = generateRecords(230, stations, dates, statuses);
                    for (StationStatusMsgDTO record : records) {
                        writer.writeToParquet(record);
                    }
                    Thread.sleep(1000);
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 0, 1, TimeUnit.SECONDS); // Generate records every second

        // Sleep for a while to simulate program running
        int totalSum = 0;
        try {
            Thread.sleep(201_000); // Simulate running for 200 seconds
            totalSum = writer.printRemainingItems();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executorService.shutdown();

        System.out.println("Total items To Be Written = " + totalRecords);
        System.out.println("None-Written Data = " + totalSum + " items!");

    }
    /*************** End of Test *****************/

}
