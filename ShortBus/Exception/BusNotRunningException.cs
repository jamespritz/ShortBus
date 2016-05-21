using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus {
    public class BusNotRunningException : Exception {
        public BusNotRunningException() {
        }

        public BusNotRunningException(string message) : base(message) {
        }

        public BusNotRunningException(string message, Exception innerException) : base(message, innerException) {
        }

        protected BusNotRunningException(SerializationInfo info, StreamingContext context) : base(info, context) {
        }
    }
}
