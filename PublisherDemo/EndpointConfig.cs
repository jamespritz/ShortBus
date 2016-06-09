using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShortBusService.Configuration;

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
                .AsAPublisher
                .MaxThreads(4)
                .Default(new ShortBus.Default.DefaultPublisherSettings() {
                    MongoConnectionString = mongoAddress
                });
        }
    }
}
