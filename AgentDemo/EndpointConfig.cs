using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShortBusService.Configuration;
using ShortBus.Default;

namespace AgentDemo {
    public class EndpointConfig : ShortBusService.Configuration.IServiceConfig {

        string endPointAddress = System.Configuration.ConfigurationManager.AppSettings["endPointAddress"];
        string mongoAddress = System.Configuration.ConfigurationManager.AppSettings["mongoAddress"];

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
                     , EndPointType = ShortBus.Publish.EndPointTypeOptions.Agent
                     , Name = ShortBus.Bus.ApplicationName
                })
                .MaxThreads(4)
                .RegisterEndpoint("Default", new RESTEndPoint(new RESTSettings(@"http://localhost:9876", ShortBus.Publish.EndPointTypeOptions.Publisher)))
                .RouteMessage<ShortBus.TestMessage>("Default", false);


        }
    }
}
