var AWS = require('aws-sdk');
var config = require('config');
var http = require('http');
var fs = require('fs');
var path = require('path');


setInterval(Main, 2000);

function Main() {
    AWS.config.update({region: config.get("aws.region")});
    process.env.AWS_ACCESS_KEY_ID = config.get("aws.credentials.id");
    process.env.AWS_SECRET_ACCESS_KEY = config.get("aws.credentials.key");
    var sqs = new AWS.SQS({apiVersion: '2012-11-05'});
    var queueURL = config.get("aws.queue_url");
    var params = {
        AttributeNames: [
            "SentTimestamp"
        ],
        MaxNumberOfMessages: 10,
        MessageAttributeNames: [
            "All"
        ],
        QueueUrl: queueURL,
        VisibilityTimeout: 0,
        WaitTimeSeconds: 0
    };

    sqs.receiveMessage(params, function (err, data) {
        if (err) {
            console.log("Receive Error", err);
        } else if (data.Messages) {
            var deleteParams = {
                QueueUrl: queueURL,
                ReceiptHandle: data.Messages[0].ReceiptHandle
            };
            var temp;
            var link;
            data.Messages.forEach(function (item, i, arr) {
                temp = JSON.parse(data.Messages[i].Body);
                temp = JSON.parse(temp.Message);
                var link_key = temp.Records[0].s3.object.key;
                link = config.get("aws.link") + link_key;

                var file = fs.createWriteStream(config.get("data_path")+path.basename(link_key));
                var request = http.get(link, function (response) {
                    response.pipe(file);
                });

                // sqs.deleteMessage(params, function (err, data) {
                //     if (err) console.log(err, err.stack);
                //     else console.log(data);
                // });
            })
        }
    });
}