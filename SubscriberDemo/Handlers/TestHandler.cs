using Newtonsoft.Json;
using ShortBus;
using ShortBus.Persistence;
using ShortBus.Subscriber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SubscriberDemo.Handlers {
    public class TestHandler : IMessageHandler {
        HandlerResponse IMessageHandler.Handle(PersistedMessage message) {
            Random r = new Random();
            int t = r.Next(100, 500);
            Thread.Sleep(t);
            TestMessage m = JsonConvert.DeserializeObject<TestMessage>(message.PayLoad);
            Console.WriteLine("Received {0}", m.Property);

            return HandlerResponse.Handled();
        }

        bool IMessageHandler.Parallel {
            get {

                return false;
            }
        }
    }
}
