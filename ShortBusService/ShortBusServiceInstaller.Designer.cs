using System.Collections.Generic;
using System.ServiceProcess;
using System.Linq;


namespace ShortBusService {
    partial class ShortBusServiceInstaller {

        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;
        protected ServiceProcessInstaller ServiceProcessInstaller1;
        protected ServiceInstaller ServiceInstaller1;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
            if (disposing && (components != null)) {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        private void InitializeComponent() {
            this.ServiceProcessInstaller1 = new System.ServiceProcess.ServiceProcessInstaller();
            this.ServiceInstaller1 = new System.ServiceProcess.ServiceInstaller();
            // 
            //ServiceProcessInstaller1 
            // 
            this.ServiceProcessInstaller1.Account = ServiceAccount.LocalSystem;

            // 
            //ServiceInstaller1 
            // 
            this.BeforeInstall += new System.Configuration.Install.InstallEventHandler(ShortBusServiceInstaller_BeforeInstall);
            this.BeforeUninstall += new System.Configuration.Install.InstallEventHandler(ShortBusServiceInstaller_BeforeUninstall);
            //string serviceName = "ARIFeedService";

            //this.ServiceInstaller1.ServiceName = serviceName;// "ARIFeedService";
            //this.ServiceInstaller1.Description = "CEI UAT ARIFeed Service";
            // 
            //ProjectInstaller 
            // 
            this.Installers.AddRange(new System.Configuration.Install.Installer[] { this.ServiceProcessInstaller1, this.ServiceInstaller1 });

        }

        void ShortBusServiceInstaller_BeforeUninstall(object sender, System.Configuration.Install.InstallEventArgs e) {
            System.Console.WriteLine("*************************************************************");

            System.Configuration.Install.Installer x = (sender as System.Configuration.Install.Installer);

            string sname = "ShortBus Endpoint";
            string sdesc = "Short Bus Endpoint";
            string dname = "Short Bus Endpoint";

            List<KeyValuePair<string, string>> paramsAsList = new List<KeyValuePair<string, string>>();
            foreach (string k in x.Context.Parameters.Keys) {
                paramsAsList.Add(new KeyValuePair<string, string>(k, x.Context.Parameters[k]));
            }

            string keyValue = null;
            keyValue = paramsAsList.FirstOrDefault(g => g.Key.Equals("servicename", System.StringComparison.OrdinalIgnoreCase)).Value;
            if (!string.IsNullOrEmpty(keyValue)) { sname = keyValue; }

            keyValue = paramsAsList.FirstOrDefault(g => g.Key.Equals("servicedescription", System.StringComparison.OrdinalIgnoreCase)).Value;
            if (!string.IsNullOrEmpty(keyValue)) { sdesc = keyValue; }

            keyValue = paramsAsList.FirstOrDefault(g => g.Key.Equals("displayname", System.StringComparison.OrdinalIgnoreCase)).Value;
            if (!string.IsNullOrEmpty(keyValue)) { dname = keyValue; }

            this.ServiceInstaller1.ServiceName = sname;
            this.ServiceInstaller1.Description = sdesc;
            this.ServiceInstaller1.DisplayName = dname;
        }

        void ShortBusServiceInstaller_BeforeInstall(object sender, System.Configuration.Install.InstallEventArgs e) {
            System.Console.WriteLine("*************************************************************");

            System.Configuration.Install.Installer x = (sender as System.Configuration.Install.Installer);

            string sname = "ShortBusSubscriber";
            string sdesc = "Short Bus Subscriber";
            string dname = "Short Bus Subscriber";

            List<KeyValuePair<string, string>> paramsAsList = new List<KeyValuePair<string, string>>();
            foreach (string k in x.Context.Parameters.Keys) {
                paramsAsList.Add(new KeyValuePair<string, string>(k, x.Context.Parameters[k]));
            }

            string keyValue = null;
            keyValue = paramsAsList.FirstOrDefault(g => g.Key.Equals("servicename", System.StringComparison.OrdinalIgnoreCase)).Value;
            if (!string.IsNullOrEmpty(keyValue)) { sname = keyValue; }

            keyValue = paramsAsList.FirstOrDefault(g => g.Key.Equals("servicedescription", System.StringComparison.OrdinalIgnoreCase)).Value;
            if (!string.IsNullOrEmpty(keyValue)) { sdesc = keyValue; }

            keyValue = paramsAsList.FirstOrDefault(g => g.Key.Equals("displayname", System.StringComparison.OrdinalIgnoreCase)).Value;
            if (!string.IsNullOrEmpty(keyValue)) { dname = keyValue; }
        }


        #endregion

    }
}