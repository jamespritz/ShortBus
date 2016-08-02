using ShortBus.Publish;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Subscriber {

    public interface IMessageHandlerFactory {

        IMessageHandler GetMessageHandler(string handlerTypeName);

    }

    public interface IMessageHandler: IEndPoint {

        bool Parallel { get; }
        HandlerResponse Handle(Persistence.PersistedMessage message);

    }
}
