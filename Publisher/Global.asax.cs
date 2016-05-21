using ShortBus;

using ShortBus.Persistence;
using ShortBus.Publish;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Web;
using System.Web.Hosting;
using System.Web.Mvc;
using System.Web.Routing;

namespace Publisher
{

    //    public class BusDriver : IRegisteredObject {

    //        public BusDriver() { }

    //        public void StartBus() {

    //            IDistributor p = new Distributor();
    //            Bus.Configure.DisableStartupTests().ApplicationName("Test Publisher")
    //                .AsAPublisher.Default(new ShortBus.Default.DefaultPublisherSettings() {
    //                    MongoConnectionString = @"mongodb://127.0.0.1:27017"
    //                }
                                   
    //            ).RegisterMessage("SandBox.Test, SandBox", "Default")
    //            .RegisterDistributor("Default", p).MaxThreads(4);

    //            Bus.OnStarted += onStarted;
    //            Bus.OnProcessing += onProcessing;
    //            Bus.OnStalled += onStalled;
    //            Bus.OnThreadStarted += onThreadStarted;
    //            Bus.Start();

    //            HostingEnvironment.RegisterObject(this);
    //        }

    //        private void onThreadStarted(object sender, EventArgs args) {
    //            //do nothing
    //        }

    //        private void onProcessing(object sender, EventArgs args) {
    //            //do nothing
    //        }

    //        private void onStalled(object sender, EventArgs args) {
    //            //do nothing
    //        }

    //        private void onStarted(object sender, EventArgs args) {
    //            //do nothing
    //        }

    //        public void Stop(bool immediate) {
    //            Bus.Stop();
    //            HostingEnvironment.UnregisterObject(this);
    //        }
    //}


    public class MvcApplication : System.Web.HttpApplication
    {

        
        //public BusDriver driver;

        protected void Application_Start()
        {
            AreaRegistration.RegisterAllAreas();
            RouteConfig.RegisterRoutes(RouteTable.Routes);

            //driver = new BusDriver();
            //driver.StartBus();
        }
    }




}
