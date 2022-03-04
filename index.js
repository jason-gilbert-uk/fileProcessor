var g_bucketName;
var g_queueName;
var g_ConfigTableName;
var g_productTableName;


const {readObjectFromSQS} = require('@jasongilbertuk/sqs-helper');
const {writeItemToTable} = require('@jasongilbertuk/dynamo-helper');
const {readObjectFromS3} = require('@jasongilbertuk/s3-helper');

async function processMessage(message) {
    try {
        var result = await writeItemToTable(g_productTableName,message);
        console.log('writeItemToDB result: ',result)
        return result;
    } catch (err) {
        console.log('error on writeItemToDB. Err = ',err)
    }
}

async function processFile(msg) {
    const bucket = msg.bucket;
    const fileName = msg.file;
    var messages = await readObjectFromS3(bucket,fileName);
    var messages2 = JSON.parse(messages.Body.toString('utf-8'));
    console.log('messages length = ',messages2.length)
    for (var i=0;i<messages2.length;i++) {
        console.log('writing message #',i)
        try {
            var result = await processMessage(messages2[i]);
            
        } catch (err) {
            console.log('error on call to processMessage. Err = ',err)
        }
    }
    return "complete"
}

async function queueReader() {
    
    var result = undefined
    var start = new Date()
    var end = new Date();
    const TIMEOUT = 120000; // 2 minutes in milliseconds
    

    while(end-start < TIMEOUT) {
        while (result === undefined && end-start < TIMEOUT) {
            result = await readObjectFromSQS(g_queueName)
            end = new Date();
            console.log(end-start);
        }
        if (result !== undefined) {
            const messages = result.Messages;
            console.log('queue length = ',messages.length)
            for (var i=0; i< messages.length; i++) {
                var msg = JSON.parse(messages[i].Body);
                try {
                    console.log('queue write #',i)
                    var result = await processFile(msg);
                } catch(err) {
                    console.log('error on await processFile(msg): ',err)
                }
            }
            start = new Date(); 
        }
        result = undefined;
    }
}


async function fileProcessor(dbConfigTableName,dbProductTableName,bucketName,queueName) {
    g_bucketName = bucketName
    var g_queueName = queueName
    var g_ConfigTableName = dbConfigTableName
    var g_productTableName = dbProductTableName
    try {
        queueReader();

    } catch(err) {
        console.log('Error encountered : ',err)
    }
}

exports.fileProcessor = fileProcessor;