using ShortBus.Default;
using ShortBusService.Configuration;
using SubscriberDemo.Handlers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SubscriberDemo {
    public class EndpointConfig : IServiceConfig {

        string endPointAddress = System.Configuration.ConfigurationManager.AppSettings["endPointAddress"];
        string mongoAddress = System.Configuration.ConfigurationManager.AppSettings["mongoAddress"];
        string pubEndPointAddress = System.Configuration.ConfigurationManager.AppSettings["pubEndPointAddress"];

        string IServiceConfig.EndPointAddress {
            get {
                return endPointAddress;
            }
        }

        void IServiceConfig.ConfigureBus() {
            ShortBus.Bus.Configure
                 .PersistTo(new MongoPersistProvider(mongoAddress, MongoDataBaseName.UseExisting("ShortBus")))
                 .MyEndPoint(new ShortBus.Configuration.EndPoint() {
                     EndPointAddress = endPointAddress
                     , Name = ShortBus.Bus.ApplicationName
                     , EndPointType = ShortBus.Publish.EndPointTypeOptions.Subscriber
                 })
                 .AsASubscriber
                 .MaxThreads(4)
                 .RegisterPublisher("Default", new RESTEndPoint(new RESTSettings() { URL = pubEndPointAddress }))
                 
                 //.Default(new ShortBus.Default.DefaultSubscriberSettings() {
                 //    MongoConnectionString = mongoAddress
                 //    , Endpoint = new ShortBus.Default.RESTSettings() {
                 //        URL = endPointAddress
                 //    }, Publisher = new ShortBus.Default.RESTSettings() { URL = pubEndPointAddress }
                 //})
             .RegisterSubscription<ShortBus.TestMessage>("Default", true)
             .RegisterMessageHandler<ShortBus.TestMessage>(new TestHandler());
        }
    }
}
