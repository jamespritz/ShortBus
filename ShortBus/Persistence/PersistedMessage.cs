using ShortBus.Publish;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Persistence {

    
    public enum PersistedMessageStatusOptions {
        Uncommitted = -1,
        ReadyToProcess = 0,
        Marked = 1,
        Processing = 2,
        Exception = 3
    }

    //handler, type, date, retrycount
    public class Route {
        public string EndPointName { get; set; }
        public DateTime Routed { get; set; }
        public EndPointTypeOptions EndPointType { get; set; }

    }

    public class PersistedMessage {

        
        public PersistedMessage() {
            this.Id = Guid.NewGuid();
            this.TransactionID = this.Id.ToString();
            this.Routes = new List<Route>();
            this.RetryCount = 0;

        }
        public PersistedMessage(string payLoad) {
            this.Id = Guid.NewGuid();
            this.TransactionID = this.Id.ToString();
            this.PayLoad = payLoad;
            this.Routes = new List<Route>();
            this.RetryCount = 0;

        }
        public string Queue { get; set; }
        public Guid Id { get;set; }

        public int RetryCount { get; set; }

        public DateTime DateStamp { get; set; }

        public string PayLoad { get;set; }
        public Dictionary<string,string> Headers = new Dictionary<string, string>();

        public List<Route> Routes { get; set; }

        public int Ordinal { get; set; }
        public string TransactionID { get; set; }
        public string MessageType { get;set; }

        /// <summary>
        /// null = new message
        /// 1 = marked
        /// 
        /// </summary>
        public PersistedMessageStatusOptions Status { get;set; }

        public PersistedMessage Clone() {

            PersistedMessage toReturn =  new PersistedMessage() {

                Headers = this.Headers
                , Id = Guid.NewGuid()
                , MessageType = this.MessageType
                , Ordinal = this.Ordinal
                , PayLoad = this.PayLoad
                , Status = this.Status
                , DateStamp = this.DateStamp
                , Queue = this.Queue
                , TransactionID = this.TransactionID
                
            };

            this.Routes.ForEach(g => {
                toReturn.Routes.Add(g);
            });

            return toReturn;

        }
    }
}
