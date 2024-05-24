// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}

using System;
using System.Text.Json;
using Azure.Messaging;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace IntegrationWorks.Function
{
    public class delivery_update_sub
    {
        private readonly ILogger<delivery_update_sub> _logger;

        public delivery_update_sub(ILogger<delivery_update_sub> logger)
        {
            _logger = logger;
        }

        [Function(nameof(delivery_update_sub))]
        public async Task RunAsync([EventGridTrigger] CloudEvent cloudEvent)
        {
            string? connectionString = Environment.GetEnvironmentVariable("DELIVERY_UPDATE_QUEUE_KEY");
            string? queueName = Environment.GetEnvironmentVariable("DELIVERY_UPDATE_QUEUE_NAME");
            ServiceBusClient client;
            ServiceBusProcessor processor;
            client = new ServiceBusClient(connectionString);
            // create a processor that we can use to process the messages
            processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions());
            _logger.LogInformation("Event type: {type}, Event subject: {subject}", cloudEvent.Type, cloudEvent.Subject);
            try
            {
                _logger.LogInformation("READING MESSAGES");
                // add handler to process messages
                processor.ProcessMessageAsync += MessageHandler;

                // add handler to process any errors
                processor.ProcessErrorAsync += ErrorHandler;

                // start processing 
                await processor.StartProcessingAsync();
                await Task.Delay(TimeSpan.FromSeconds(3));
                await processor.StopProcessingAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError("Error processing deliveryUpdate: {deliveryUpdate}", ex.Message);
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await processor.DisposeAsync();
                await client.DisposeAsync();
                _logger.LogInformation("Disposing of processor and client!");
            }
        }
        // handle received messages
        async Task MessageHandler(ProcessMessageEventArgs args)
        {
            _logger.LogInformation("IN MESSAGE HANDLER");
            string body = args.Message.Body.ToString();

            if (body != null)
            {
                try
                {
                    DeliveryUpdate? deliveryUpdate = JsonSerializer.Deserialize<DeliveryUpdate>(body);
                    if (deliveryUpdate != null)
                    {
                        string status = deliveryUpdate.Status;
                        _logger.LogInformation("Received: {body} with status : {status}", body, status);
                    }
                    else
                    {
                        _logger.LogError("deliveryUpdate is null!");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError("Error processing deliveryUpdate: {deliveryUpdate}", ex.Message);
                }
            }
            else
            {
                _logger.LogError("Error processing deliveryUpdate: Message body was null.");
            }
            await args.CompleteMessageAsync(args.Message);
        }

        // handle any errors when receiving messages
        Task ErrorHandler(ProcessErrorEventArgs args)
        {
            _logger.LogError("Receiving deliveryUpdate errors: {err}", args.Exception.ToString());
            return Task.CompletedTask;
        }
    }
}
