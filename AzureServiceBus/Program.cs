using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AzureServiceBus
{
    class Program
    {
        // Set Azure Bus configuration
        const string ServiceBusConnectionString = "";
        const string TopicName = "";
        const string SubscriptionName = "";
        static ISubscriptionClient subscriptionClient;
        static ITopicClient topicClient;

        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            try
            {
                topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

                Console.WriteLine(">> Type a message to send to Azure Service Bus.");
                Console.WriteLine("");

                string message = Console.ReadLine();
                Console.WriteLine("");

                Console.WriteLine(">> Type the correlation Id.");
                Console.WriteLine("");

                string correlationId = Console.ReadLine();
                Console.WriteLine("");

                // Send messages.

                await SendMessagesAsync(message, correlationId);

                Console.WriteLine(">> The message(s) was sent");
                Console.WriteLine("");

                await topicClient.CloseAsync();

                subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);

                Console.WriteLine(">> Press ENTER key to receive all messages in the ServiceBus.");
                Console.WriteLine("");

                Console.ReadKey();

                // Register subscription message handler and receive messages in a loop
                RegisterOnMessageHandlerAndReceiveMessages();

                await subscriptionClient.CloseAsync();

                Console.WriteLine("");

                Console.ReadKey();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {ex.Message}");
            }
        }

        private static async Task SendMessagesAsync(string message, string correlationId)
        {
            try
            {

                var msg = new Message(Encoding.UTF8.GetBytes(message));
                msg.CorrelationId = correlationId;

                // Write the body of the message to the console
                Console.WriteLine($"Sending message: {message}");
                Console.WriteLine("");

                // Send the message to the topic
                await topicClient.SendAsync(msg);

            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {ex.Message}");
            }
        }

        private static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
                AutoComplete = false
            };

            // Register the function that processes messages.
            subscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        private static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message.
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            Console.WriteLine("");
            Console.WriteLine(">> Press ENTER key to exit.");

            // Complete the message so that it is not received again.
            // This can be done only if the subscriptionClient is created in ReceiveMode.PeekLock mode (which is the default).
            await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);

            // Note: Use the cancellationToken passed as necessary to determine if the subscriptionClient has already been closed.
            // If subscriptionClient has already been closed, you can choose to not call CompleteAsync() or AbandonAsync() etc.
            // to avoid unnecessary exceptions.
        }

        private static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            Console.WriteLine("");
            return Task.CompletedTask;
        }


    }
}
