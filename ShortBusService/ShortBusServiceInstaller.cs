using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;
using System.Threading.Tasks;

namespace ShortBusService {
    [RunInstaller(true)]
    public partial class ShortBusServiceInstaller : System.Configuration.Install.Installer {
        public ShortBusServiceInstaller() {
            InitializeComponent();
        }
    }
}
