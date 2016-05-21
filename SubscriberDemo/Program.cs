using Microsoft.Owin.Hosting;
using ShortBus.Subscriber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShortBus.Persistence;
using ShortBus;
using Newtonsoft.Json;
using System.Threading;

namespace SubscriberDemo {
    class Program {
        static void Main(string[] args) {
            Console.WriteLine("Welcome to Subscriber Demo");

            using (IDisposable app = WebApp.Start<ShortBus.Hosting.WebAPI.HostConfiguration>(url: @"http://192.168.1.13:9875")) {

                ShortBus.Bus.Configure
                    .AsASubscriber
                    .MaxThreads(4)
                    .Default(new ShortBus.Default.DefaultSubscriberSettings() {
                        MongoConnectionString = @"mongodb://127.0.0.1:27017"
                        , Endpoint = new ShortBus.Default.RESTSettings() {
                            URL = @"http://192.168.1.13:9875"
                        }, Publisher = new ShortBus.Default.RESTSettings() { URL = @"http://192.168.1.13:9876" }
                    })
                .RegisterSubscription<ShortBus.TestMessage>("Default")
                .RegisterMessageHandler<ShortBus.TestMessage>(new TestHandler());
                ShortBus.Bus.Start();


                Console.ReadLine();
                ShortBus.Bus.Stop(false);
            }

        }
    }

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

                return true;
            }
        }
    }

}
