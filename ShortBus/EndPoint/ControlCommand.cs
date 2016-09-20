using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.EndPoint {
    public class ControlCommand {
        public ControlCommand() { }

        public string Command { get; set; }
        public Dictionary<string, string> Params { get; set; }

    }
}
