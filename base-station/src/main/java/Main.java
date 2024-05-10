import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import consumer.BaseStationConsumer;

public class Main {

    @Inject
    private BaseStationConsumer baseStationConsumer;

    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new BasicModule());
        Main app = injector.getInstance(Main.class);
        app.run();
    }

    public void run() {
        baseStationConsumer.consumeMessage();
    }
}
