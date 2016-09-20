using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ShortBus.EndPoint;
using ShortBus.Persistence;
using ShortBus.Publish;
using ShortBus.Routing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

namespace ShortBus.Hosting.WebAPI {
    public class MessageController : ApiController {

        [HttpPost]
        public RouteResponse PostMessage(PersistedMessage message) {

            RouteResponse toReturn = new RouteResponse() { Status = false };
            try {
                toReturn = Bus.ReceiveMessage(message);
                

            } catch {
                
            }

            return toReturn;

        }

        [HttpGet]
        public RouteResponse CC(ControlCommand command) {

            //serialize message
            string serialized = JsonConvert.SerializeObject(command, new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.All });
            JObject asJO = JObject.Parse(serialized);
            


            //create peristed message
            BusMessage msg = new BusMessage() { PayLoad = serialized, MessageType = asJO["$type"].ToString() };


            RouteResponse toReturn = new RouteResponse() { Status = false };

            try {
                toReturn = Bus.ReceiveMessage(msg);   
            } catch {
                throw;
            }

            return toReturn;

        }

    }
}
