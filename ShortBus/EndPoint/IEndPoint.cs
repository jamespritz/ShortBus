using ShortBus.Persistence;
using ShortBus.Routing;
using ShortBus.Subscriber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Publish {




    public enum EndPointTypeOptions {
        Source = 1, Publisher = 2, Subscriber = 3, Handler = 4
    };

    public struct EndPointResponse {
        public EndPointTypeOptions EndPointType { get; set; }
        public RouteResponse RouteResponse { get; set; }
        public HandlerResponse HandlerResponse { get; set; }
        public bool Status { get; set; }
    }

    public interface IEndPoint {

        EndPointTypeOptions EndPointType { get; }


    }
}
