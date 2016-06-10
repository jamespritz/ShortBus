using ShortBusService.Configuration;
using ShortBusService.Host;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace ShortBusService {
    static class Program {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main() {

            //this has to happen first, otherwise the call to Environment will load the exe's config as default, at which point
            //we can't overwrite with the dlls.
            System.Configuration.Configuration exeConfig = System.Configuration.ConfigurationManager.OpenExeConfiguration(Assembly.GetExecutingAssembly().Location);
            string endPointAddress = exeConfig.AppSettings.Settings["EndpointType"].Value;

            //string endPointAddress = System.Configuration.ConfigurationManager.AppSettings["EndpointType"];
            string assemblyName = endPointAddress.Split(',')[1].Trim();
            string typeName = endPointAddress.Split(',')[0].Trim();
            Assembly endpoint = Assembly.LoadFrom(string.Format("{0}.dll", assemblyName));
            Type ofConfig = endpoint.GetType(typeName);

            //reset the apps config file to that of the endpoint dll's.
            System.Configuration.Configuration f = System.Configuration.ConfigurationManager.OpenExeConfiguration(endpoint.Location);
            AppDomain.CurrentDomain.SetData("APP_CONFIG_FILE", f.FilePath);

            EndpointHost.SetConfig((IServiceConfig)Activator.CreateInstance(ofConfig));


            //if we are running as console, don't start the service
            if (Environment.UserInteractive) {

                using (IDisposable app = Host.EndpointHost.StartHost()) {

                    Console.WriteLine("{0} Running!", ShortBus.Bus.ApplicationName);
                    Console.WriteLine("Press any key to stop");
                    Console.ReadKey();
                    ShortBus.Bus.Stop(false);

                }

                    

            } else {
                ServiceBase[] ServicesToRun;
                ServicesToRun = new ServiceBase[]
                {
                    new ShortBusService()
                };
                ServiceBase.Run(ServicesToRun);
            }


        }
    }
}
