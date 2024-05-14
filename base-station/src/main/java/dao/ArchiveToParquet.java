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
    private static final int BATCH_SIZE = 10_000;
    private Map<Long, Map<String, List<GenericData.Record>>> buffers;

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
    private void writeRecord(GenericData.Record record, Long stationId, String date) throws IOException {
        List<GenericData.Record> buffer = buffers.get(stationId).get(date);
        buffer.add(record);
        if(buffer.size() >= BATCH_SIZE) {
            String fileId = UUID.randomUUID().toString();
            String fileToWritePath = ARCHIVE_DIRECTORY +
                    "station_" + stationId + "/" +
                    "day_" + date + "/" + fileId + ".parquet";
            /* Write Batch */
            writeFile(buffer, fileToWritePath);
            /* Flush the Record */
            buffer.clear();
        }
    }

    private void writeFile(List<GenericData.Record> buffer, String path) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        ParquetWriter<GenericData.Record> writer;
        writer = AvroParquetWriter
                .<GenericData.Record>builder(new Path(path))
                .withSchema(STATUS_SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build();
        try {
            for(GenericData.Record record : buffer) {
                writer.write(record);
            }
        } finally {
            writer.close();
        }
    }

    /*************** TESTING *****************/
//    static int totalRecords = 0;
//    private int printRemainingItems() {
//        int totalSum = 0;
//        for(Map.Entry<Long, Map<String, List<GenericData.Record>>> entry : buffers.entrySet()) {
//            System.out.print("Station_" + entry.getKey() + " --> ");
//            int sum = 0;
//            for(Map.Entry<String, List<GenericData.Record>> inner : entry.getValue().entrySet()) {
//                sum += inner.getValue().size();
//            }
//            System.out.println(sum);
//            totalSum += sum;
//        }
//        return totalSum;
//    }
//    private static List<StationStatusMsgDTO> generateRecords(int count, long[] stations, long[] dates, String[] statuses) throws NoSuchFieldException, IllegalAccessException {
//        List<StationStatusMsgDTO> objects = new ArrayList<>();
//        Random random = new Random();
//        for (int i = 0; i < count; i++) {
//            int stationIdx = random.nextInt(0,10);
//            int dateIdx = random.nextInt(0,2);
//            int statusIdx = random.nextInt(0,3);
//            StationStatusMsgDTO record = new StationStatusMsgDTO(stations[stationIdx], random.nextLong(), statuses[statusIdx], dates[dateIdx],
//                    new WeatherDTO(random.nextInt(0, 100), random.nextInt(0,100), random.nextInt(0,100)));
//            objects.add(record);
//            totalRecords++;
//        }
//        return objects;
//    }
//
//    public static void main(String[] args) {
//        long[] stations = new long[] {11L, 12L, 13L, 14L, 15L, 16, 17L, 18L, 19L, 20L};
//        long[] dates = new long[] {1715678151279L, 1715579709000L}; // today , yesterday
//        String[] statuses = new String[] {"LOW", "MEDIUM", "HIGH"};
//        ArchiveToParquet writer = new ArchiveToParquet();
//        /* Testing writing in Batches */
//        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
//        executorService.scheduleAtFixedRate(() -> {
//            try {
//                List<StationStatusMsgDTO> records = generateRecords(50, stations, dates, statuses);
//                for (StationStatusMsgDTO record : records) {
//                    writer.writeToParquet(record);
//                }
//            } catch (NoSuchFieldException | IllegalAccessException e) {
//                e.printStackTrace();
//            }
//        }, 0, 1, TimeUnit.SECONDS); // Generate records every second
//
//        // Sleep for a while to simulate program running
//        int totalSum = 0;
//        try {
//            Thread.sleep(10100); // Simulate running for 60 seconds
//            totalSum = writer.printRemainingItems();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        executorService.shutdown();
//
//        System.out.println("Total items To Be Written = " + totalRecords);
//        System.out.println("None-Written Data = " + totalSum + " items!");
//
//    }
    /*************** End of Test *****************/

}
