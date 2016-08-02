using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Publish {


    public class EndpointPingRequest {
        public string EndPoint { get; set; }
        public string GUID { get; set; }
        public string Name { get; set; }
        public EndPointTypeOptions EndPointType { get; set; }
    }

    public class EndpointRegistrationRequest {
        public string EndPoint { get; set; }
        public string GUID { get; set; }
        public string Name { get; set; }
        public EndPointTypeOptions EndPointType { get; set; }
        public bool DeRegister { get; set; }
    }

    public class SubscriptionRequest: EndpointRegistrationRequest {

        public string MessageTypeName { get; set; }
        public bool DiscardIfSubscriberIsDown { get; set; }


    }




}
