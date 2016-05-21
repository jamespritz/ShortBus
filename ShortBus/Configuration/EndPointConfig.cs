using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Configuration {



    public class EndPointConfig {
        public string Version { get; set; }
        public string ApplicationName { get; set; }
        public string ApplicationGUID { get; set; }
    }
    public class EndPointDBConfig {
        public EndPointDBConfig() { }
        public string Version { get; set; }
    }

    public class SourceEndPointConfig:EndPointConfig {
        public SourceEndPointConfig() { }
        
    }

    public class PublisherEndPointConfig: EndPointConfig {
        public PublisherEndPointConfig() { }
        //list of known subscribers (named) and their endpoint configuration
        public Dictionary<string,string> Subscribers { get; set; }
        //mappings of message type to subscriber name
        public List<MessageTypeMapping> Subscriptions { get; set; }

    }

    public class SubscriberEndPointConfig: EndPointConfig {
        public SubscriberEndPointConfig() { }

    }

}
