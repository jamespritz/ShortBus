//using Microsoft.Owin.Hosting;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace PublisherDemo {
//    class Program {
//        static void Main(string[] args) {
//            Console.WriteLine("Welcome to Publisher Demo");




//            using (IDisposable app = WebApp.Start<ShortBus.Hosting.WebAPI.HostConfiguration>(url:endPointAddress)) {


//                ShortBus.Bus.Configure
//                    .AsAPublisher
//                    .MaxThreads(4)
//                    .Default(new ShortBus.Default.DefaultPublisherSettings() {
//                        MongoConnectionString = mongoAddress
//                    });
//                ShortBus.Bus.Start();

                
//                Console.ReadLine();
//                ShortBus.Bus.Stop(false);

//            }
//        }
//    }
//}
