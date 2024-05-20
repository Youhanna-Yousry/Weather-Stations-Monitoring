package dao.Impl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import dao.ParquetDAO;
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
import org.joda.time.LocalDate;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class ParquetDAOImpl implements ParquetDAO {

    private Schema STATUS_SCHEMA;
    private Schema WEATHER_SCHEMA;
    private static final String STATUS_SCHEMA_LOCATION = "src/main/resources/archiving_files/avroSchema.avsc";
    private static final String WEATHER_SCHEMA_LOCATION= "src/main/resources/archiving_files/innerAvroSchema.avsc";
    public static final String ARCHIVE_DIRECTORY = "/mnt/parquet/";
    private static final int BATCH_SIZE = 1000; // batch size is 10k, but when testing small functionalities: we may need to change this value.
    private Map<Long, Map<String, List<GenericData.Record>>> buffers;
    private int buffersSize;
    private final Logger LOGGER;

    @Inject
    public ParquetDAOImpl(@Named("ParquetLogger") Logger LOGGER) {
        buffers = new HashMap<>();
        buffersSize = 0;
        this.LOGGER = LOGGER;
        createDirectory();
        defineSchemas();
    }

    private void createDirectory() {
        if (!new File(ARCHIVE_DIRECTORY).mkdir()) {
            LOGGER.error("Failed to create the parquet directory");
        }
    }

    private void defineSchemas() {
        try(InputStream statusStream = new FileInputStream(STATUS_SCHEMA_LOCATION);
            InputStream weatherStream= new FileInputStream(WEATHER_SCHEMA_LOCATION))
        {
            STATUS_SCHEMA = new Schema.Parser().parse(IOUtils.toString(statusStream, StandardCharsets.UTF_8));
            WEATHER_SCHEMA = new Schema.Parser().parse(IOUtils.toString(weatherStream, StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOGGER.error("Can't read SCHEMA file from {}", STATUS_SCHEMA_LOCATION);
            throw new RuntimeException(e);
        }
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
        LocalDate localDate = new LocalDate(new Date(time));
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
                    for(GenericData.Record record : innerEntry.getValue()) {
                        writer.write(record);
                    }
                } finally {
                    writer.close();
                }
            }
        }
        buffers = new HashMap<>();
        buffersSize = 0;
    }

}
