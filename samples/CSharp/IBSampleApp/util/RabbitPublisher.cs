using System;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace IBSampleApp.util
{
    public static class RabbitPublisher
    {
        private static readonly IConnection connection;
        private static readonly IModel channel;
        private const string QueueName = "marketdata";

        static RabbitPublisher()
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            connection = factory.CreateConnection();
            channel = connection.CreateModel();
            channel.QueueDeclare(queue: QueueName,
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);
        }

        public static void Publish(string symbol, double bidPrice, double askPrice, DateTime time)
        {
            var payload = new
            {
                local_symbol = symbol,
                bidprice = bidPrice,
                askprice = askPrice,
                time = time.ToString("o")
            };
            var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(payload));
            channel.BasicPublish(exchange: string.Empty,
                                 routingKey: QueueName,
                                 basicProperties: null,
                                 body: body);
        }
    }
}
