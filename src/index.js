const { SQSClient, ReceiveMessageCommand, SendMessageCommand, DeleteMessageCommand } = require("@aws-sdk/client-sqs");
const { fromIni } = require("@aws-sdk/credential-provider-ini");

const region = process.env.AWS_REGION;
const profile = process.env.AWS_CREDENTIALS_PROFILE;
const maxMessages = process.env.MAX_MESSAGES;
const queueURL = process.env.AWS_QUEUE_URL;
const dlqUrl = `${queueURL}-dlq`;

const DLQ_RETRIES_ATTRIBUTE = 'dlqRetries';

(async () => {
  try {
    const sqsClient = new SQSClient({ region, credentials: fromIni({ profile }) });

    let input = {
      QueueUrl: dlqUrl,
      MaxNumberOfMessages: maxMessages,
      MessageAttributeNames: ['All']
    }

    const receiveMessageCommand = new ReceiveMessageCommand(input);
    let response = await sqsClient.send(receiveMessageCommand);
    if (response.Messages) {
      for (const message of Object.values(response.Messages)) {
        console.log(message);
        
        const receiptHandle = message.ReceiptHandle;
        let retries = 1;
        if (message.MessageAttributes) {
          const extractedRetries = message.MessageAttributes[DLQ_RETRIES_ATTRIBUTE].StringValue;
          console.log(extractedRetries);
          retries = parseInt(extractedRetries) + 1;
        }
        console.log('current_retries', retries);

        const messageAttributesSend = {};
        messageAttributesSend[DLQ_RETRIES_ATTRIBUTE] = {
          StringValue: retries.toString(),
          DataType: 'Number'
        }

        input = {
          QueueUrl: queueURL,
          MessageBody: message.Body,
          MessageAttributes: messageAttributesSend
        }
        response = await sqsClient.send(new SendMessageCommand(input));
        console.log('sent');

        input = {
          QueueUrl: dlqUrl,
          ReceiptHandle: receiptHandle
        }
        response = await sqsClient.send(new DeleteMessageCommand(input));
        console.log('deleted');        
      }
      return;

    }
    console.log('no messages');
  } catch(error) {
    console.error(error);
  }

})();

