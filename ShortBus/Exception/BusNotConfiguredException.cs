using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus {
    public class BusNotConfiguredException : Exception {

        internal string msg = string.Empty;
        public BusNotConfiguredException(string asA, string message) {
            msg = string.Format("Bus is misconfigured as a {0} - {1}", asA, message);
            
        }

        public override string Message {
            get {
                return msg;
            }
        }


        protected BusNotConfiguredException(SerializationInfo info, StreamingContext context) : base(info, context) {
        }
    }
}
