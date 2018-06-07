const AWS = require('aws-sdk');
const config = require('config');
const http = require('http');
const fs = require('fs');
const path = require('path');

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

setInterval(checking, 1000);

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

            data.Messages.forEach(function (item, i) {
                let duplicate = false;
                deleteParams.Entries.forEach(function (item1, i) {
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
                let link_key = temp.Records[0].s3.object.key;
                link = config.get("aws.link") + link_key;

                let file = fs.createWriteStream(config.get("data_path") + path.basename(link_key));
                let request = http.get(link, function (response) {
                    response.pipe(file);
                });
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
    if (processingData === false) {
        processingData = true;
        main();
    }
}