using Microsoft.Owin.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShortBus;
using ShortBusService.Configuration;
using System.Reflection;

namespace ShortBusService.Host {
    class EndpointHost {

        internal static IServiceConfig config = null;
        internal static void SetConfig(IServiceConfig loadedConfig) {
            
            config = loadedConfig;
        }

        

        internal static IDisposable StartHost() {

            
            string endpointAddress = config.EndPointAddress;
            ShortBus.Bus.Configure.SetApplicationGUID(ShortBus.Util.Util.GetApplicationGuid(config.GetType().Assembly));
            ShortBus.Bus.Configure.SetApplicationName(ShortBus.Util.Util.GetApplicationName(config.GetType().Assembly));

            //endpoint is reponsible for configuring bus
            config.ConfigureBus();

            ShortBus.Bus.Start();

            endpointAddress = endpointAddress.ToLower().Replace("localhost", ShortBus.Util.Util.GetLocalIP());
            return WebApp.Start<ShortBus.Hosting.WebAPI.HostConfiguration>(url: endpointAddress);

        }

    }
}
