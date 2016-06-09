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

        static IServiceConfig config = null;
        static IServiceConfig GetConfig() {
            if (config == null) {
                //get class to load from config
                //load assembly and get IServiceConfig implementation
                // 
                
                string endPointAddress = System.Configuration.ConfigurationManager.AppSettings["EndpointType"];
                string assemblyName = endPointAddress.Split(',')[1].Trim();
                string typeName = endPointAddress.Split(',')[0].Trim();
                Assembly endpoint = Assembly.LoadFrom(string.Format("{0}.dll", assemblyName));
                Type ofConfig = endpoint.GetType(typeName);

                //reset the apps config file to that of the endpoint dll's.
                System.Configuration.Configuration f = System.Configuration.ConfigurationManager.OpenExeConfiguration(endpoint.Location);
                AppDomain.CurrentDomain.SetData("APP_CONFIG_FILE", f.FilePath);


                config = (IServiceConfig)Activator.CreateInstance(ofConfig);
                

            }
            return config;
        }

        public static void StartHost() {

            IServiceConfig hostConfig = GetConfig();
            string endpointAddress = hostConfig.EndPointAddress;
            using (IDisposable app = WebApp.Start<ShortBus.Hosting.WebAPI.HostConfiguration>(url: endpointAddress )) {

                //endpoint is reponsible for configuring bus
                hostConfig.ConfigureBus();

                ShortBus.Bus.Start();

            }

        }

    }
}
