using ShortBus.Publish;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Configuration {



    public class EndPointConfigBase {
        public string Version { get; set; }
        public string ApplicationName { get; set; }
        public string ApplicationGUID { get; set; }
       
    }

    public class EndPoint {
        public string Name { get; set; }
        public EndPointTypeOptions EndPointType { get; set; }
        public string EndPointAddress { get; set; }

    }

    public class EndPointConfig: EndPointConfigBase {
        //endpoints (name, address, type)
        //subscriptions
        public List<EndPoint> EndPoints { get; set; }
        private List<MessageTypeMapping> subscriptions;
        public List<MessageTypeMapping> Subscriptions {  get {
                if (this.subscriptions == null) {
                    return new List<MessageTypeMapping>();

                } else return this.subscriptions;
            }
            set {
                this.subscriptions = value;
            }
        }

    }

    public static class EndPointConfigExtensions {
        public static IEnumerable<EndPoint> Subscribers(this EndPointConfig config) {
            if (config.EndPoints == null) {
                return new List<EndPoint>().AsEnumerable();

            } else {
                return (from g in config.EndPoints where g.EndPointType == EndPointTypeOptions.Subscriber select g);
            }
        }
        public static IEnumerable<EndPoint> Sources(this EndPointConfig config) {
            if (config.EndPoints == null) {
                return new List<EndPoint>().AsEnumerable();

            } else {
                return (from g in config.EndPoints where g.EndPointType == EndPointTypeOptions.Source select g);
            }
        }
        public static IEnumerable<EndPoint> Publishers(this EndPointConfig config) {
            if (config.EndPoints == null) {
                return new List<EndPoint>().AsEnumerable();

            } else {
                return (from g in config.EndPoints where g.EndPointType == EndPointTypeOptions.Publisher select g);
            }
        }

        public static void AddEndPoint(this EndPointConfig config, EndPoint toAdd) {
            if (config.EndPoints == null) {
                config.EndPoints = new List<EndPoint>();
            }

            if (config.EndPoints.Exists(g => g.Name.Equals(toAdd.Name, StringComparison.OrdinalIgnoreCase))) {
                throw new InvalidOperationException(string.Format("EndPoint: {0} already exists", toAdd.Name));
            }
            config.EndPoints.Add(toAdd);
            
        }

        public static void RemoveEndPoint(this EndPointConfig config, string name) {
            if (config.EndPoints != null) {
                EndPoint match = config.EndPoints.FirstOrDefault(g => g.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
                   
                if (match != null) {
                    config.EndPoints.Remove(match);
                }
            }
        }

    }



    //public class SourceEndPointConfig:EndPointConfig {
    //    public SourceEndPointConfig() { }
        
    //}

    //public class PublisherEndPointConfig: EndPointConfig {
    //    public PublisherEndPointConfig() { }
    //    //list of known subscribers (named) and their endpoint configuration
    //    public Dictionary<string,string> Subscribers { get; set; }
    //    //mappings of message type to subscriber name
    //    public List<MessageTypeMapping> Subscriptions { get; set; }

    //}

    //public class SubscriberEndPointConfig: EndPointConfig {
    //    public SubscriberEndPointConfig() { }

    //}

}
