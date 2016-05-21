using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Web;

namespace ShortBus.Util {
    internal class Util {
    
        public static string GetTypeName(Type t) {

            string fullName = t.FullName;

            AssemblyName a = t.Assembly.GetName();

            return string.Format("{0}, {1}", fullName, a.Name);

        }

        private static Assembly GetMainAssembly() {
            if (HttpContext.Current != null) {
                var type = HttpContext.Current.ApplicationInstance.GetType();
                while (type != null && type.Namespace == "ASP") {
                    type = type.BaseType;
                }

                return type == null ? null : type.Assembly;
            } else {
                return Assembly.GetEntryAssembly();
            }
        }

        public static string GetApplicationName() {
            return GetMainAssembly().GetName().Name;
            
        }

        public static string GetApplicationGuid() {
            Assembly assembly = GetMainAssembly();
            var attribute = (GuidAttribute)assembly.GetCustomAttributes(typeof(GuidAttribute),true)[0];
            var id = attribute.Value;
            return id;
        }

        /*
         * 


        
System.AppDomain.CurrentDomain.FriendlyName - Returns the filename with extension (e.g. MyApp.exe).

System.Diagnostics.Process.GetCurrentProcess().ProcessName - Returns the filename without extension (e.g. MyApp).

System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName - 
         */

    }
}