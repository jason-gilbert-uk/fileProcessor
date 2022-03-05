const {readObjectFromSQS} = require('@jasongilbertuk/sqs-helper');
const {writeItemToProductTable} = require('@jasongilbertuk/product-table');
const {readObjectFromS3} = require('@jasongilbertuk/s3-helper');

var g_bucketName;
var g_queueName;
var g_ConfigTableName;
var g_productTableName;

async function processMessage(message) {
    try {
        var result = await writeItemToProductTable(g_productTableName,message);
        return result;
    } catch (err) {
        console.log('fileProcessor encounted error on writeItemToTable. Err = ',err)
        throw err;
    }
}

async function processFile(msg) {
    const bucket = msg.bucket;
    const fileName = msg.file;
    var messages = await readObjectFromS3(bucket,fileName);
    for (var i=0;i<messages.length;i++) {
        try {
            var result = await processMessage(messages[i]);
        } catch (err) {
            console.log('error on call to processMessage. Err = ',err)
            throw err
        }
    }
}

async function queueReader() {
    var start = new Date()
    var end = new Date();
    const TIMEOUT = 120000; // 2 minutes in milliseconds
    
    // Basically, if we go for 2 minutes without receiving anything, processing ends.
    while(end-start < TIMEOUT) {
        var result = undefined
        while (result === undefined && end-start < TIMEOUT) {
            result = await readObjectFromSQS(g_queueName)
            end = new Date();
            console.log(end-start);
        }
        if (result !== undefined) {
            // Result is an array of messages
            // [{bucket: 'name of bucket',file: 'name of file'}]
            const messages = result.Messages;
            for (var i=0; i< messages.length; i++) {
                var msg = JSON.parse(messages[i].Body);
                try {
                    var result = await processFile(msg);
                } catch(err) {
                    console.log('error on await processFile(msg): ',err)
                    throw err;
                }
            }
            start = new Date(); // Reset start time. This will allow another 2 minutes.
        }
    }
}


async function fileProcessor(dbConfigTableName,dbProductTableName,bucketName,queueName) {
    g_bucketName = bucketName
    g_queueName = queueName
    g_ConfigTableName = dbConfigTableName
    g_productTableName = dbProductTableName
    try {
        queueReader();

    } catch(err) {
        console.log('Error encountered in call to queueReader : ',err)
        throw err;
    }
}

exports.fileProcessor = fileProcessor;