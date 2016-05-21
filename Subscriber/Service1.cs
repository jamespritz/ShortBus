using Microsoft.Owin.Hosting;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Subscriber {
    public partial class Service1 : ServiceBase {
        public Service1() {
            InitializeComponent();
        }



        protected override void OnStart(string[] args) {

            //initialize web host
            //configure bus
            //inform bus to start

            WebApp.Start<Infrastructure.HostConfiguration>(url: @"http://localhost:9876");
            

        }

        protected override void OnStop() {


            //inform bus to stop
            //stop web host

        }
    }
}
