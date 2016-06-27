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
           


            TestMessage m = JsonConvert.DeserializeObject<TestMessage>(message.PayLoad);
            if (message.HandleRetryCount == 0) {
                Console.WriteLine("Recieved {0}, rescheduling", m.Property);
                return HandlerResponse.Retry(TimeSpan.FromMinutes(1));
            } else {
                Console.WriteLine("processing {0}", m.Property);
                return HandlerResponse.Handled();
            }
          
        }

        bool IMessageHandler.Parallel {
            get {

                return false;
            }
        }
    }
}
