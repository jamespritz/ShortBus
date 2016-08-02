using ShortBus.Persistence;
using ShortBus.Publish;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Routing {

    public class RouteResponse {
        public RouteResponse() { }
        public bool Status { get; set; }
        public string PayLoad { get; set; }

    }
    public interface IMessageRouter: IEndPoint {
        /// <summary>
        /// Called by the Sender to publish a message.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        RouteResponse Publish(PersistedMessage message);


        //Called by the bus to Test if publisher endpoint is up.
        RouteResponse HelloWorld(PersistedMessage message);

        /// <summary>
        /// The Bus will query this property before initiating any requests, and handle gracefully.
        /// Implementation should maintain this flag and wait for a reset request.
        /// </summary>
        /// <returns></returns>
        bool ServiceIsDown();

        /// <summary>
        /// On request, the Bus may request that the publisher attempt to reset. This operation should
        /// attempt a reconnect, and return true or false. if true, should reset the 'ServiceIsDown' flag
        /// so Bus can continue.
        /// </summary>
        /// <returns></returns>
        bool ResetConnection();
        bool ResetConnection(string endpointAddress);
    }
}
