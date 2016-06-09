using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBusService.Configuration {
    public interface IServiceConfig {
        void ConfigureBus();
        string EndPointAddress { get; }

    }
}
