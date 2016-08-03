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
                 .MaxThreads(4)
                 .RegisterEndpoint("Default", new RESTEndPoint(new RESTSettings(pubEndPointAddress, ShortBus.Publish.EndPointTypeOptions.Publisher )))
                 .SubscribeToMessage<ShortBus.TestMessage>("Default")
                
                .RegisterMessageHandler<ShortBus.TestMessage>(new TestHandler());
        }
    }
}
