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

            Bus.ReceiveMessage(message);

            return new RouteResponse() { Status = true };

        }

        [HttpGet]
        public string GetMessage(int id) {
            return "hello";
        }

    }
}
