using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus {
    public class ServiceEndpointDownException : Exception {
        public ServiceEndpointDownException() {

        }

        public ServiceEndpointDownException(string message) : base(message) {
            
        }

        public ServiceEndpointDownException(string message, Exception innerException) : base(message, innerException) {
        }

        protected ServiceEndpointDownException(SerializationInfo info, StreamingContext context) : base(info, context) {
        }
    }
}
