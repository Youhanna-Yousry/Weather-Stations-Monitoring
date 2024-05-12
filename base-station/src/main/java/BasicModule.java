import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import consumer.BaseStationConsumer;
import consumer.Impl.BaseStationConsumerImpl;
import dao.BitcaskDAO;
import dao.Impl.BitcaskDAOImpl;
import mapper.Mapper;
import mapper.MapperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.BaseStationService;
import service.Impl.BaseStationServiceImpl;

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

        bind(Mapper.class).to(MapperImpl.class);

        bind(BaseStationConsumer.class).to(BaseStationConsumerImpl.class);
        bind(BaseStationService.class).to(BaseStationServiceImpl.class);
        bind(BitcaskDAO.class).to(BitcaskDAOImpl.class);
    }
}
