using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace ShortBusService {
    public partial class ShortBusService : ServiceBase {
        public ShortBusService() {
            InitializeComponent();
        }

        protected override void OnStart(string[] args) {
        }

        protected override void OnStop() {
            base.OnStop();
        }
    }
}
