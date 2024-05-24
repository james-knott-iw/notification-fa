using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace IntegrationWorks.Function
{
    public class DeliveryStatus
    {
        public required string Status { get; set; }
    }
    public class DeliveryUpdate
    {
        public required DateTime Time { get; set; }
        public required string Status { get; set; }
    }
    public class delivery_update
    {
        private readonly ILogger<delivery_update> _logger;

        public delivery_update(ILogger<delivery_update> logger)
        {
            _logger = logger;
        }

        [Function("delivery_update")]
        public async Task<IActionResult> RunAsync([HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequest req)
        {
            // Read request body and deserialize body into a DeliveryStatus Object
            string requestBody;
            using (StreamReader reader = new StreamReader(req.Body))
            {
                requestBody = await reader.ReadToEndAsync();
            }

            _logger.LogDebug(requestBody);
            DeliveryStatus? deliveryStatus;
            try
            {
                deliveryStatus = JsonSerializer.Deserialize<DeliveryStatus>(requestBody);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                _logger.LogError("Incorrectly formatted request Body", ex);
                return new OkObjectResult("Incorrectly formatted request Body");
            }

            //Get Service Bus Queue Information
            string? connectionString = Environment.GetEnvironmentVariable("DELIVERY_UPDATE_QUEUE_KEY");
            string? queueName = Environment.GetEnvironmentVariable("DELIVERY_UPDATE_QUEUE_NAME");

            // the client that owns the connection and can be used to create senders and receivers
            ServiceBusClient client;

            // the sender used to publish messages to the queue
            ServiceBusSender sender;
            client = new ServiceBusClient(connectionString);

            // create a processor that we can use to process the messages
            sender = client.CreateSender(queueName);

            // Process and send DeliveryUpdate Message to the Service Bus Queue
            using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();
            DeliveryUpdate deliveryUpdate = new DeliveryUpdate { Status = deliveryStatus.Status, Time = DateTime.Now };

            if (!messageBatch.TryAddMessage(new ServiceBusMessage(JsonSerializer.Serialize(deliveryUpdate))))
            {
                //Failed to add Message to the message batch
                _logger.LogError($"Exception {deliveryUpdate} has ocurred.");
            }
            try
            {
                //Send message batch to the Service Bus queue
                await sender.SendMessagesAsync(messageBatch);
                _logger.LogInformation($"Sent message: {deliveryUpdate} to the queue");
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }

            _logger.LogInformation("C# HTTP trigger function processed a request.");
            return new OkObjectResult("Delivery Update Sent!");
        }
    }
}
