using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShortBusService.Configuration;
using ShortBus.Default;

namespace PublisherDemo {
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
                     , EndPointType = ShortBus.Publish.EndPointTypeOptions.Source
                     , Name = ShortBus.Bus.ApplicationName
                })
                .AsAPublisher
                .MaxThreads(4);
                
        }
    }
}
