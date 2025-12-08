package utb.fai;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import utb.fai.API.HumiditySensor;
import utb.fai.API.IrrigationSystem;
import utb.fai.Types.FaultType;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Trida MQTT klienta pro mereni vhlkosti pudy a rizeni zavlazovaciho systemu.
 *
 * V teto tride implementuje MQTT klienta
 */
public class SoilMoistureMQTTClient {
    private static final long HUMIDITY_PUBLISH_INTERVAL_SECONDS = 10;
    private static final long IRRIGATION_TIMEOUT_SECONDS = 30;

    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> humidityTask;
    private ScheduledFuture<?> irrigationTimeoutTask;

    private MqttClient client;
    private HumiditySensor humiditySensor;
    private IrrigationSystem irrigationSystem;

    /**
     * Vytvori instacni tridy MQTT klienta pro mereni vhlkosti pudy a rizeni
     * zavlazovaciho systemu
     *
     * @param sensor     Senzor vlhkosti
     * @param irrigation Zarizeni pro zavlahu pudy
     */
    public SoilMoistureMQTTClient(HumiditySensor sensor, IrrigationSystem irrigation) {
        this.humiditySensor = sensor;
        this.irrigationSystem = irrigation;
        this.scheduler = Executors.newScheduledThreadPool(2);
    }

    /**
     * Metoda pro spusteni klienta
     */
    public void start() {
        try {
            client = new MqttClient(Config.BROKER, Config.CLIENT_ID);
            client.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    try {
                        client.subscribe(Config.TOPIC_IN);
                    } catch (MqttException e) {
                        System.err.println("Subscription failed: " + e.getMessage());
                    }
                }

                @Override
                public void connectionLost(Throwable cause) {
                    System.err.println("MQTT connection lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, org.eclipse.paho.client.mqttv3.MqttMessage message) {
                    handleInboundMessage(new String(message.getPayload(), StandardCharsets.UTF_8));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) { }
            });

            client.connect();
            publishHumidityValue();
            scheduleHumidityPublishing();
        } catch (MqttException e) {
            throw new RuntimeException("Failed to start MQTT client", e);
        }
    }

    private void handleInboundMessage(String payload) {
        String trimmed = payload == null ? "" : payload.trim();
        if (trimmed.isEmpty()) {
            return;
        }
        switch (trimmed) {
            case Config.REQUEST_GET_HUMIDITY:
                publishHumidityValue();
                break;
            case Config.REQUEST_GET_STATUS:
                publishStatus();
                break;
            case Config.REQUEST_START_IRRIGATION:
                startIrrigation();
                break;
            case Config.REQUEST_STOP_IRRIGATION:
                stopIrrigation();
                break;
            default:
                System.err.println("Unknown command: " + trimmed);
        }
    }

    private void scheduleHumidityPublishing() {
        cancelHumidityTask();
        humidityTask = scheduler.scheduleAtFixedRate(this::publishHumidityValue,
                HUMIDITY_PUBLISH_INTERVAL_SECONDS,
                HUMIDITY_PUBLISH_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
    }

    private void publishHumidityValue() {
        float humidity = humiditySensor.readRAWValue();
        if (humiditySensor.hasFault()) {
            publishFault(FaultType.HUMIDITY_SENSOR_FAULT);
            return;
        }
        publishOut(String.format(Locale.US, "%s;%.2f", Config.RESPONSE_HUMIDITY, humidity));
    }

    private void startIrrigation() {
        if (irrigationSystem.hasFault()) {
            publishFault(FaultType.IRRIGATION_SYSTEM_FAULT);
            return;
        }
        if (!irrigationSystem.isActive()) {
            cancelIrrigationTimeout();
            irrigationSystem.activate();
            if (irrigationSystem.hasFault()) {
                publishFault(FaultType.IRRIGATION_SYSTEM_FAULT);
                return;
            }
            scheduleIrrigationTimeout();
        }
        publishStatus();
    }

    private void stopIrrigation() {
        cancelIrrigationTimeout();
        irrigationSystem.deactivate();
        if (irrigationSystem.hasFault()) {
            publishFault(FaultType.IRRIGATION_SYSTEM_FAULT);
            return;
        }
        publishStatus();
    }

    private void scheduleIrrigationTimeout() {
        cancelIrrigationTimeout();
        irrigationTimeoutTask = scheduler.schedule(this::stopIrrigation,
                IRRIGATION_TIMEOUT_SECONDS,
                TimeUnit.SECONDS);
    }

    private void publishStatus() {
        var status = irrigationSystem.isActive() ? "irrigation_on" : "irrigation_off";
        publishOut(String.format("%s;%s", Config.RESPONSE_STATUS, status));
    }

    private void publishFault(FaultType faultType) {
        publishOut(String.format("%s;%s", Config.RESPONSE_FAULT, faultType));
    }

    private void publishOut(String payload) {
        try {
            client.publish(Config.TOPIC_OUT, payload.getBytes(StandardCharsets.UTF_8), 0, false);
        } catch (MqttException e) {
            System.err.println("Failed to publish MQTT message: " + e.getMessage());
        }
    }

    private void cancelHumidityTask() {
        if (humidityTask != null) {
            humidityTask.cancel(false);
            humidityTask = null;
        }
    }

    private void cancelIrrigationTimeout() {
        if (irrigationTimeoutTask != null) {
            irrigationTimeoutTask.cancel(false);
            irrigationTimeoutTask = null;
        }
    }
}
