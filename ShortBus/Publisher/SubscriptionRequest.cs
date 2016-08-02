using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Publish {

    public struct SubscriptionRequest {
        public string MessageTypeName { get; set; }
        public bool DiscardIfSubscriberIsDown { get; set; }
    }

    public class EndpointRegistrationRequest {

        public EndpointRegistrationRequest() {
            this.SubscriptionRequests = new List<SubscriptionRequest>();
        }

        public string EndPoint { get; set; }
        public string GUID { get; set; }
        public string Name { get; set; }
        public EndPointTypeOptions EndPointType { get; set; }
        public bool DeRegister { get; set; }

        public List<SubscriptionRequest> SubscriptionRequests { get; set; }

    }





}
