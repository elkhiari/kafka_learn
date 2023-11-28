const express = require('express');
const Kafka = require('kafka-node');

const app = express();
const port = 3000;

app.use(express.json());

const Producer = Kafka.Producer;
const client = new Kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new Producer(client);

producer.on('ready', () => {
    console.log('Kafka Producer is connected and ready.');
});

producer.on('error', (error) => {
    console.error(`Kafka Producer error: ${error}`);
});

app.get('/', (req, res) => {
    res.send('Hello World!');
});

app.post('/send', (req, res) => {
    if (!req.body || !req.body.message) {
        return res.status(400).json({"message":'Bad Request: Message is required in the request body.'});
    }

    const message = req.body.message;

    const payloads = [
        {
            topic: 'ftp',
            messages: message
        }
    ];

    producer.send(payloads, (error, data) => {
        if (error) {
            console.error(`Kafka send error: ${error}`);
             res.status(500).json({"message":'Internal Server Error: Could not send message.'});
        }

        else res.status(200).json({"message":'Message sent successfully.',data});
    })
});

app.listen(port, () => {
    console.log(`Express app listening at http://localhost:${port}`);
});
