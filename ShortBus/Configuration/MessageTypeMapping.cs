using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Configuration {

    public enum MessageDirectionOptions {
        Inbound = 1, Outbound = 2
    }

    public class MessageTypeMapping {

        public MessageTypeMapping(string typeName, string endPointName, MessageDirectionOptions direction, bool discardIfDown) {
            this.TypeName = typeName;
            this.Direction = direction;
            this.DiscardIfDown = discardIfDown;
            this.EndPointName = endPointName;
        }

        public string TypeName { get; set; }
        public string EndPointName { get; set; }
        public MessageDirectionOptions Direction { get; set; }
        public bool DiscardIfDown { get; set; }
    }
}
