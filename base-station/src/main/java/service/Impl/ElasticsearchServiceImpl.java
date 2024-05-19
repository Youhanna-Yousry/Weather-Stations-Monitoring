package service.Impl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import service.ElasticsearchService;

public class ElasticsearchServiceImpl implements ElasticsearchService {

    private static final String PYTHON_SCRIPT = "src/main/python/elastic_indexing_pull_mode.py";

    @Inject
    @Named("ElasticsearchLogger")
    private Logger logger;

    @Override
    public void start() {
        ProcessBuilder pb = new ProcessBuilder("python3", PYTHON_SCRIPT);
        try {
            logger.info("Starting the python script of elasticsearch");
            pb.start();
        } catch (Exception e) {
            logger.error("Error while running the python script of elasticsearch: ", e);
        }
    }
}
