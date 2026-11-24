import {Aedes} from "aedes";
import {createServer} from "node:net";

const broker = await Aedes.createBroker();
const server = createServer(broker.handle);

server.listen(0, "127.0.0.1", () => {
    process.send({port: server.address().port});
});

process.on("message", message => {
    if (message === "shutdown") {
        server.close(() => broker.close(() => process.exit(0)));
    }
});
