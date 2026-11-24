import {fork, ChildProcess} from "child_process";
import {once} from "events";
import {Characteristic, Service} from "hap-nodejs";
import {connectAsync, MqttClient} from "mqtt";
import {join} from "path";
import {MQTTPublishObject, MQTTSubscribeObject, parseMQTTOptions} from "../configparser";
import {MQTTClient} from "./mqttClient";

describe("MQTT compatibility", () => {
    let broker: ChildProcess;
    let port: number;
    let peer: MqttClient;
    let client: MQTTClient;
    let service: Service;
    const log = Object.assign(jest.fn(), {error: jest.fn(), warn: jest.fn(), info: jest.fn()});

    beforeEach(async () => {
        broker = fork(join(__dirname, "../test/mqtt-broker.mjs"), [], {stdio: ["ignore", "ignore", "inherit", "ipc"]});
        const [message] = await Promise.race([
            once(broker, "message"),
            once(broker, "exit").then(([code]) => { throw new Error(`MQTT broker exited: ${code}`); }),
        ]);
        port = (message as {port: number}).port;
        peer = await connectAsync(`mqtt://127.0.0.1:${port}`);
        service = new Service.Switch("Test switch");
        client = new MQTTClient(service, {host: "127.0.0.1", port, subscriptions: []}, log);
    });

    afterEach(async () => {
        if (client?.client) {
            await new Promise<void>(resolve => client.end(true, () => resolve()));
        }
        if (peer) {
            await peer.endAsync(true);
        }
        if (broker && broker.exitCode === null) {
            const exited = once(broker, "exit");
            broker.send("shutdown");
            await exited;
        }
        jest.clearAllMocks();
    });

    it("connects and delivers subscription messages to a HomeKit characteristic", async () => {
        const subscription = new MQTTSubscribeObject("switch/state");
        subscription.qos = 1;
        subscription.messagePattern = "^ON$";
        client.subscribe(subscription, "On");
        client.connect();
        const connected = once(client, "connected");
        const subscribed = new Promise<void>(resolve => {
            client.client!.on("packetreceive", packet => {
                if (packet.cmd === "suback") resolve();
            });
        });
        await connected;
        await subscribed;

        const characteristic = service.getCharacteristic(Characteristic.On);
        jest.spyOn(service, "getCharacteristic").mockImplementation(() => {
            throw new Error("This HomeKit service does not resolve characteristic names");
        });
        const changed = once(characteristic, "change");
        await peer.publishAsync("switch/state", "ON", {qos: 1});
        const [change] = await changed;
        expect(change.newValue).toBe(true);
        expect(characteristic.value).toBe(true);
    });

    it("subscribes after connection and lets plugins transform received values", async () => {
        client.connect();
        await once(client, "connected");
        const subscription = new MQTTSubscribeObject("switch/live");
        subscription.messagePattern = "^ON$";
        const subscribed = new Promise<void>(resolve => {
            client.client!.on("packetreceive", packet => {
                if (packet.cmd === "suback") resolve();
            });
        });
        client.subscribe(subscription, "On");
        await subscribed;
        client.on("message-On", (value, callback, characteristic) => {
            expect(value).toBe(true);
            expect(characteristic).toBe("On");
            callback(false);
        });
        const characteristic = service.getCharacteristic(Characteristic.On);
        const changed = once(characteristic, "change");
        await peer.publishAsync("switch/live", "ON");
        const [change] = await changed;
        expect(change.newValue).toBe(false);
    });

    it("publishes formatted retained messages using the configured QoS", async () => {
        client.connect();
        await once(client, "connected");
        await peer.subscribeAsync("switch/set", {qos: 1});
        const published = new MQTTPublishObject("switch/set");
        published.qos = 1;
        published.retain = true;
        published.payloadFormatter = (value: string) => `value:${value}`;
        const received = new Promise<[string, Buffer]>(resolve => {
            peer.once("message", (topic, payload) => resolve([topic, payload]));
        });
        await new Promise<void>((resolve, reject) => {
            client.publish(published, "ON", error => error ? reject(error) : resolve());
        });
        const [topic, payload] = await received;
        expect(topic).toBe("switch/set");
        expect(payload.toString()).toBe("value:ON");

        await peer.unsubscribeAsync("switch/set");
        const retained = new Promise<{payload: Buffer, retain: boolean, qos: number}>(resolve => {
            peer.once("message", (_topic, payload, packet) => resolve({payload, retain: packet.retain, qos: packet.qos}));
        });
        await peer.subscribeAsync("switch/set", {qos: 1});
        const packet = await retained;
        expect(packet.payload.toString()).toBe("value:ON");
        expect(packet.retain).toBe(true);
        expect(packet.qos).toBe(1);
    });

    it("parses configured subscriptions and extracts numeric characteristic values", async () => {
        service = new Service.Lightbulb("Test light");
        const characteristic = service.getCharacteristic(Characteristic.Brightness);
        const options = parseMQTTOptions({
            host: "127.0.0.1",
            port,
            subscriptions: [{topic: "light/state", characteristic: "Brightness", messagePattern: "level=(\\d+)"}],
        });
        client = new MQTTClient(service, options, log);
        client.connect();
        const subscribed = new Promise<void>(resolve => {
            client.client!.on("packetreceive", packet => {
                if (packet.cmd === "suback") resolve();
            });
        });
        await subscribed;

        const received = new Promise<void>(resolve => client.client!.once("message", () => resolve()));
        await peer.publishAsync("light/state", "unmatched");
        await received;
        expect(log.error).toHaveBeenCalledWith(expect.stringContaining("couldn't extract value"));

        const changed = once(characteristic, "change");
        await peer.publishAsync("light/state", "level=43");
        const [change] = await changed;
        expect(change.newValue).toBe(43);
    });

    it("resubscribes after reconnecting to the broker", async () => {
        const subscription = new MQTTSubscribeObject("switch/reconnect");
        subscription.messagePattern = "^ON$";
        client.subscribe(subscription, "On");
        client.connect();
        const firstSubscription = new Promise<void>(resolve => {
            client.client!.once("packetreceive", packet => {
                if (packet.cmd === "connack") {
                    client.client!.once("packetreceive", () => resolve());
                }
            });
        });
        await firstSubscription;

        const reconnected = once(client, "connected");
        const resubscribed = new Promise<void>(resolve => {
            client.client!.on("packetreceive", packet => {
                if (packet.cmd === "suback") resolve();
            });
        });
        client.client!.reconnect();
        await reconnected;
        await resubscribed;
        const characteristic = service.getCharacteristic(Characteristic.On);
        const changed = once(characteristic, "change");
        await peer.publishAsync("switch/reconnect", "ON");
        const [change] = await changed;
        expect(change.newValue).toBe(true);
    });
});
