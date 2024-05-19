package service.Impl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import service.ElasticsearchService;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ElasticsearchServiceImpl implements ElasticsearchService {

    private static final String ARCHIVE_PATH = "src/main/resources/archiving_files/archive";
    private final Set<Path> parquetFiles = new HashSet<>();
    private static final String SCHEMA_JSON = "{"
            + "\"type\": \"record\","
            + "\"name\": \"StationStatusMsgDTO\","
            + "\"namespace\": \"dto\","
            + "\"fields\": ["
            + "{\"name\": \"stationId\", \"type\": \"long\"},"
            + "{\"name\": \"sequenceNumber\", \"type\": \"long\"},"
            + "{\"name\": \"batteryStatus\", \"type\": \"string\"},"
            + "{\"name\": \"statusTimestamp\", \"type\": \"long\"},"
            + "{\"name\": \"weather\", \"type\": {"
            + "\"type\": \"record\","
            + "\"name\": \"WeatherDTO\","
            + "\"fields\": ["
            + "{\"name\": \"humidity\", \"type\": \"int\"},"
            + "{\"name\": \"temperature\", \"type\": \"int\"},"
            + "{\"name\": \"windSpeed\", \"type\": \"int\"}"
            + "]}}"
            + "]}";

    @Inject
    @Named("ElasticsearchLogger")
    private Logger logger;

    private Schema avroSchema;

    @Override
    public void start() {
        avroSchema = new Schema.Parser().parse(SCHEMA_JSON);
        Thread thread = new Thread(this::indexForGood);
        thread.start();
    }

    private void indexForGood() {
        while (true) {
            indexParquetFiles();
        }
    }

    private void indexParquetFiles () {
        try {
            List<Path> parquetFiles = findParquetFiles(ARCHIVE_PATH);

            for (Path parquetFile : parquetFiles) {
                if (this.parquetFiles.contains(parquetFile)) {
                    continue;
                }
                List<String> records = readRecords(parquetFile.toString());
                indexRecords(records);
                this.parquetFiles.add(parquetFile);
            }
        } catch (IOException e) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                logger.error("Failed to sleep", ex);
            }
            logger.error("Failed to index parquet files", e);
        }
    }

    public static List<Path> findParquetFiles(String startDir) throws IOException {
        try (Stream<Path> stream = Files.walk(Paths.get(startDir), FileVisitOption.FOLLOW_LINKS)) {
            return stream
                    .filter(file -> !Files.isDirectory(file))
                    .filter(file -> file.toString().endsWith(".parquet"))
                    .collect(Collectors.toList());
        }
    }

    private List<String> readRecords(String parquetFile) throws IOException {
        List<String> records = new ArrayList<>();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(parquetFile)).withDataModel(GenericData.get()).build()) {
            GenericRecord nextRecord;

            while ((nextRecord = reader.read()) != null) {
                records.add(nextRecord.toString());
            }
        } catch (Exception e) {
            logger.error("Failed to read parquet file: " + parquetFile, e);
            throw new IOException("Failed to read parquet file", e);
        }

        return records;
    }

    private void indexRecords(List<String> records) {
        try (RestClient restClient = RestClient.builder(new HttpHost("elasticsearch-service", 9200, "http")).build()) {
            Response response = null;
            for (String record : records) {
                HttpEntity entity = new NStringEntity(record, ContentType.APPLICATION_JSON);
                Request request = new Request("POST", "/data/_doc");
                request.setEntity(entity);
                response = restClient.performRequest(request);
            }
            logger.info("Record indexed: {}", response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            logger.error("Failed to index records", e);
        }
    }

    public static void main(String[] args) {
        ElasticsearchServiceImpl service = new ElasticsearchServiceImpl();
        service.start();
    }
}
