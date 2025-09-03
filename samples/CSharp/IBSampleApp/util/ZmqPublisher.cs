using System;
using Newtonsoft.Json;
using NetMQ;
using NetMQ.Sockets;

namespace IBSampleApp.util
{
    public static class ZmqPublisher
    {
        private static readonly PublisherSocket publisher;

        static ZmqPublisher()
        {
            publisher = new PublisherSocket();
            publisher.Bind("tcp://*:5555");
        }

        public static double CalculateSpread(double bidPrice, double askPrice)
        {
            return Math.Abs(askPrice - bidPrice);
        }

        public static void Publish(string symbol, double bidPrice, double askPrice, DateTime time)
        {
            var payload = new
            {
                local_symbol = symbol,
                bidprice = bidPrice,
                askprice = askPrice,
                spread = CalculateSpread(bidPrice, askPrice),
                time = time.ToString("o")
            };
            publisher.SendFrame(JsonConvert.SerializeObject(payload));
        }
    }
}
