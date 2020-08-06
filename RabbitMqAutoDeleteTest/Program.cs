using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqAutoDeleteTest
{
    public class Program
    {
        private static IModel consumerChannel;
        private static IConnection consumerConnection;
        private static string queueName = "test-queue";
        private static string exchangeName = "test-exchange";
        private static IConnectionFactory factory;

        public static void Main(string[] args)
        {
            var exitEvent = new ManualResetEventSlim(false);
            Console.CancelKeyPress += (sender, eventArgs) => {
                eventArgs.Cancel = true;
                exitEvent.Set();
            };
            factory = new ConnectionFactory
            {
                Uri = new Uri("amqp://tester:password@localhost:5672"),
                AutomaticRecoveryEnabled = true,
                DispatchConsumersAsync = true
            };

            Setup();
            StartConsumer(factory, queueName);
            Console.WriteLine("Consumer started...");

            exitEvent.Wait();
            StopConsumer();
        }

        private static void Setup()
        {
            var connection = factory.CreateConnection("setup-connection");

            var channel = connection.CreateModel();

            channel.ExchangeDeclare(exchangeName, "direct", true, false, null);
            channel.QueueDeclare(queueName, false, false, true, null);
            channel.QueueBind(queueName, exchangeName, "#");

            channel.Close();
            connection.Close();
        }

        private static void StartConsumer(IConnectionFactory connectionFactory, string queueName)
        {
            consumerConnection = connectionFactory.CreateConnection($"consumer-moderator-{queueName}-{Guid.NewGuid()}");

            if (consumerConnection is IAutorecoveringConnection autorecoveringConnection)
            {
                autorecoveringConnection.RecoverySucceeded += (sender, args) =>
                {
                    Console.WriteLine($"Recovery succeeded. Channel is open: {consumerChannel.IsOpen}");
                };
            }
            consumerChannel = consumerConnection.CreateModel();

            var consumer = new AsyncEventingBasicConsumer(consumerChannel);
            consumer.Received += HandleDelivery;

            consumerChannel.ModelShutdown += (sender, args) =>
            {
                Console.WriteLine("Model Shutdown {0}", args.ToString());
                if (args is ShutdownEventArgs shutdownEventArgs)
                {
                    if (shutdownEventArgs.ReplyCode == 404)
                    {
                        Task.Run(async () => await RecoverAutoDeleteQueue()).Wait();
                    }
                }
            };
            consumerChannel.BasicQos(0, 1, false);
            consumerChannel.BasicConsume(queueName, false, consumer);
        }

        private static ValueTask RecoverAutoDeleteQueue()
        {
            try
            {
                consumerChannel.Close();
                consumerChannel.Dispose();
                Setup();

                consumerChannel = consumerConnection.CreateModel();
            }
            catch (Exception iex)
            {
                Console.WriteLine($"Failed to create new channel. {iex}");
            }

            return new ValueTask();
        }

        private static void StopConsumer()
        {
            consumerChannel.Close();
            consumerConnection.Close();
            consumerChannel.Dispose();
            consumerConnection.Dispose();
        }


        private static Task HandleDelivery(object sender, BasicDeliverEventArgs ea)
        {

            var body = Encoding.UTF8.GetString(ea.Body.ToArray());
            Console.WriteLine($"Message received. Body: {body}");

            consumerChannel.BasicAck(ea.DeliveryTag, false);
            return Task.CompletedTask;
        }
    }
}