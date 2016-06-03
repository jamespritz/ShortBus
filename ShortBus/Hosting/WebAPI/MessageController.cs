using ShortBus.Persistence;
using ShortBus.Publish;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

namespace ShortBus.Hosting.WebAPI {
    public class MessageController : ApiController {

        [HttpPost]
        public EndpointResponse PostMessage(PersistedMessage message) {


            if (Bus.Configure.IsAPublisher) {
                Bus.BroadcastMessage(message);
            } else if (Bus.Configure.IsASubscriber) {
                Bus.ReceiveMessage(message);
            }
            //if i am a publisher, broadcast
            //if i am a subscriber, processMessage
            //if i am both (test cases only)... inspect message... if sent != null, then i must be a subscriber

            //ShortBus.Bus.BroadcastMessage(message);

            return new EndpointResponse() { Status = true };

        }

        [HttpGet]
        public string GetMessage(int id) {
            return "hello";
        }

    }
}
