const AWS = require('aws-sdk');
const config = require('config');
const path = require('path');
const amqp = require('amqplib/callback_api');

AWS.config.update({region: config.get("aws.region")});
process.env.AWS_ACCESS_KEY_ID = config.get("aws.credentials.id");
process.env.AWS_SECRET_ACCESS_KEY = config.get("aws.credentials.key");
const sqs = new AWS.SQS({apiVersion: '2012-11-05'});
const queueURL = config.get("aws.queue_url");
const params = {
    AttributeNames: [
        "SentTimestamp"
    ],
    MaxNumberOfMessages: 10,
    MessageAttributeNames: [
        "All"
    ],
    QueueUrl: queueURL,
    VisibilityTimeout: 10,
    WaitTimeSeconds: 20
};
let processingData = false;
let channel;
let upload_data = {
    "id": "",
    "timestamp": 0,
    "abbreviation": "",
    "image_url": "",
    "image": {},
    "storage_path": "",
    "storage_png_path": "",
    "added": 0
};
let time={};

setInterval(checking, 5000);

amqp.connect(config.get("amqp_url"), function (err, connect) {
    if (err != null) {
        console.error(err);
        process.exit(1);
    }
    connect.createChannel(function (err, ch) {
        if (err != null) {
            console.error(err);
            process.exit(1);
        }
        ch.assertQueue(config.get("queue_name"));
        channel = ch;
    });
});

function main() {
    sqs.receiveMessage(params, function (err, data) {
        if (err) {
            console.log("Receive Error", err);
        } else if (data.Messages) {
            let temp;
            let link;
            let deleteParams = {
                Entries: [],
                QueueUrl: queueURL
            };

            data.Messages.forEach(function (item) {
                let duplicate = false;
                let file_name;
                let link_key;
                deleteParams.Entries.forEach(function (item1) {
                    if (item1.Id === item.MessageId) {
                        duplicate = true;
                    }
                });
                if (duplicate) {
                    return;
                }

                deleteParams.Entries.push({
                    Id: item.MessageId,
                    ReceiptHandle: item.ReceiptHandle
                });
                temp = JSON.parse(item.Body);
                temp = JSON.parse(temp.Message);
                link_key = temp.Records[0].s3.object.key;
                link = config.get("aws.link") + link_key;
                file_name=path.basename(link_key);
                
                upload_data.id = upload_data.abbreviation = path.basename(link_key).substr(0, 4);
                upload_data.image_url = link;
                time.year = +file_name.substr(4, 4);
                time.month = +file_name.substr(8, 2);
                time.day = +file_name.substr(10, 2);
                time.hour = +file_name.substr(13, 2);
                time.min = +file_name.substr(15, 2);
                time.sec = +file_name.substr(17, 2);
                upload_data.timestamp = Math.floor((new Date(time.year, time.month, time.day, time.hour, time.min, time.sec)).getTime() / 1000);
                upload_data.added = Math.floor((new Date()).getTime() / 1000);
                channel.sendToQueue(config.get("queue_name"), new Buffer(JSON.stringify(upload_data)));
                console.log(upload_data.image_url);
            });
            sqs.deleteMessageBatch(deleteParams, function (err, data) {
                if (err) console.log(err, err.stack);
                else console.log(data);
            });
        }
        processingData = false;
    });
}

function checking() {
    if (processingData === false && typeof channel !== 'undefined') {
        processingData = true;
        main();
    }
}