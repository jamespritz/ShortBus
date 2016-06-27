using ShortBusService.Configuration;
using ShortBusService.Host;
using System;
using System.Collections.Generic;
using System.Configuration.Install;
using System.Linq;
using System.Reflection;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ShortBusService {
    static class Program {


        public static string serviceName = "Unconfigured";
        public static string serviceDesc = "Short Bus Service Endpoint";
        public static string displayName = "Short Bus Service Endpoint";


        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args) {

      
       
            try {

                //this has to happen first, otherwise the call to Environment will load the exe's config as default, at which point
                //we can't overwrite with the dlls.
                System.Configuration.Configuration exeConfig = System.Configuration.ConfigurationManager.OpenExeConfiguration(Assembly.GetExecutingAssembly().Location);
                string endPointAddress = exeConfig.AppSettings.Settings["EndpointType"].Value;

                //string endPointAddress = System.Configuration.ConfigurationManager.AppSettings["EndpointType"];
                string assemblyName = endPointAddress.Split(',')[1].Trim();
                string typeName = endPointAddress.Split(',')[0].Trim();

               

                string assemblyFile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), string.Format("{0}.dll", assemblyName));
                Assembly endpoint = Assembly.LoadFrom(assemblyFile);
                Type ofConfig = endpoint.GetType(typeName);
                serviceName = ShortBus.Util.Util.GetApplicationName(ofConfig.Assembly);
                //reset the apps config file to that of the endpoint dll's.
                System.Configuration.Configuration f = System.Configuration.ConfigurationManager.OpenExeConfiguration(endpoint.Location);
                AppDomain.CurrentDomain.SetData("APP_CONFIG_FILE", f.FilePath);

                EndpointHost.SetConfig((IServiceConfig)Activator.CreateInstance(ofConfig));

                //if we are running as console, don't start the service
                if (Environment.UserInteractive) {

                    if (args.Length > 0) {

                        List<string> installArgs = new List<string>();
                        string arg = args.FirstOrDefault(g => g.ToLower().Contains("servicename"));
                        if (!string.IsNullOrEmpty(arg)) {
                            serviceName = arg.Split(':')[1].Replace(" ", "");
                        }
                        arg = args.FirstOrDefault(g => g.ToLower().Contains("servicedesc"));
                        if (!string.IsNullOrEmpty(arg)) {
                            serviceDesc = arg.Split(':')[1];
                        }
                        arg = args.FirstOrDefault(g => g.ToLower().Contains("displayname"));
                        if (!string.IsNullOrEmpty(arg)) {
                            displayName = arg.Split(':')[1];
                        }

                        string command = args[0].ToLower().Replace("/", "").Replace("-", "").Trim();
                        switch (command) {
                            case "install": {

                                    installArgs.Add(Assembly.GetExecutingAssembly().Location);



                                    ManagedInstallerClass.InstallHelper(installArgs.ToArray());
                                    break;
                                }
                            case "uninstall": {
                                    installArgs.Add("/u");
                                    installArgs.Add(Assembly.GetExecutingAssembly().Location);


                                    ManagedInstallerClass.InstallHelper(installArgs.ToArray());
                                    break;
                                }
                        }
                    } else {

                        using (IDisposable app = Host.EndpointHost.StartHost()) {

                            Console.WriteLine("{0} Running!", ShortBus.Bus.ApplicationName);
                            Console.WriteLine("Press any key to stop");
                            Console.ReadKey();
                            ShortBus.Bus.Stop(false);

                        }
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
            catch(Exception e) {
                throw e;
            }
        }
    }
}
