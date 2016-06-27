using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ShortBusService {
    public partial class ShortBusService : ServiceBase {
        public ShortBusService() {
           
            InitializeComponent();
        }

        IDisposable app = null;

        protected override void OnStart(string[] args) {
            app = Host.EndpointHost.StartHost();

            ShortBus.Bus.Start();
            base.OnStart(args);

        }

        protected override void OnShutdown() {
            

            ShortBus.Bus.Stop(false);
            Thread.Sleep(15000);
            base.OnShutdown();
        }

        protected override void OnStop() {
            ShortBus.Bus.Stop(true);
            if (app != null) {
                app.Dispose();
            }
            base.OnStop();
        }
    }
}
