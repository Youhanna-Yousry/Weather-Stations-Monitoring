import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import consumer.BaseStationConsumer;
import consumer.Impl.BaseStationConsumerImpl;
import dao.BitcaskDAO;
import dao.Impl.BitcaskDAOImpl;
import dao.Impl.ParquetDAOImpl;
import dao.ParquetDAO;
import mapper.Mapper;
import mapper.MapperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.BaseStationService;
import service.ElasticsearchService;
import service.Impl.BaseStationServiceImpl;
import service.Impl.ElasticsearchServiceImpl;

public class BasicModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Logger.class)
                .annotatedWith(Names.named("ConsumerLogger"))
                .toInstance(LoggerFactory.getLogger(BaseStationConsumerImpl.class));
        bind(Logger.class)
                .annotatedWith(Names.named("BitcaskLogger"))
                .toInstance(LoggerFactory.getLogger(BitcaskDAOImpl.class));
        bind(Logger.class)
                .annotatedWith(Names.named("BaseStationServiceLogger"))
                .toInstance(LoggerFactory.getLogger(BaseStationServiceImpl.class));
        bind(Logger.class)
                .annotatedWith(Names.named("ElasticsearchLogger"))
                .toInstance(LoggerFactory.getLogger(ElasticsearchServiceImpl.class));
        bind(Logger.class)
                .annotatedWith(Names.named("ParquetLogger"))
                .toInstance(LoggerFactory.getLogger(ParquetDAOImpl.class));

        bind(Mapper.class).to(MapperImpl.class);

        bind(BaseStationConsumer.class).to(BaseStationConsumerImpl.class);
        bind(BaseStationService.class).to(BaseStationServiceImpl.class);
        bind(ElasticsearchService.class).to(ElasticsearchServiceImpl.class);
        bind(BitcaskDAO.class).to(BitcaskDAOImpl.class);
        bind(ParquetDAO.class).to(ParquetDAOImpl.class);
    }
}
