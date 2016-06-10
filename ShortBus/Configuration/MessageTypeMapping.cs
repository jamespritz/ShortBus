using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Configuration {
    public class MessageTypeMapping {
        public string TypeName { get; set; }
        public string EndPointName { get; set; }
        public bool DiscardIfDown { get; set; }
    }
}
