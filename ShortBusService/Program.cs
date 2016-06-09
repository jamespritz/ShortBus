using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace ShortBusService {
    static class Program {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main() {

            //if we are running as console, don't start the service
            if (Environment.UserInteractive) {

                Host.EndpointHost.StartHost();

                Console.WriteLine("Press any key to stop");
                Console.ReadKey();
                ShortBus.Bus.Stop(false);

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
