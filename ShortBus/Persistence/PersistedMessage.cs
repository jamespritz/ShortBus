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
    
    public class PersistedMessage {

        public PersistedMessage() {
            this.Id = Guid.NewGuid();
        }
        public PersistedMessage(string payLoad) {
            this.Id = Guid.NewGuid();
            this.PayLoad = payLoad;
        }
        public string Queue { get; set; }
        public Guid Id { get;set; }
        public DateTime Sent { get;set; }
        public DateTime? Published { get;set; }
        public DateTime? Distributed { get; set; }
        public DateTime? Received { get; set; }
        public int SendRetryCount { get;set; }
        public string PayLoad { get;set; }
        public Dictionary<string,string> Headers = new Dictionary<string, string>();
        public int HandleRetryCount { get;set; }
        public string Publisher { get; set; }
        public string Subscriber { get; set; }
        public string MessageHandler { get; set; }
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

            return new PersistedMessage() {
                MessageHandler = this.MessageHandler
                , HandleRetryCount = this.HandleRetryCount
                , Headers = this.Headers
                , Id = Guid.NewGuid()
                , Distributed = this.Distributed
                , MessageType = this.MessageType
                , Ordinal = this.Ordinal
                , PayLoad = this.PayLoad
                , Published = this.Published
                , Publisher = this.Publisher
                , SendRetryCount = this.SendRetryCount
                , Sent = this.Sent
                , Status = this.Status
                , Subscriber = this.Subscriber
                , Queue = this.Queue
                , TransactionID = this.TransactionID
                , Received = this.Received
            };

        }
    }
}
