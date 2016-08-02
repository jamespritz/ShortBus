using ShortBus.Configuration;
using ShortBus.Publish;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Persistence {




    /// <summary>
    /// implementation must provide constructor that 
    /// </summary>
    public interface IPeristProvider {
        IPersist CreatePersist(IConfigure Configure);

    }

    /// <summary>
    /// Operations required to perist a message to local storage
    /// </summary>
    public interface IPersist {


        void Persist(IEnumerable<PersistedMessage> messages);

        
        PersistedMessage PeekNext(string q);

        PersistedMessage PeekAndMarkNext(string q);

        PersistedMessage Mark(Guid id);

        PersistedMessage Processing(Guid id);

        PersistedMessage Pop(Guid Id);

        PersistedMessage Reschedule(Guid id, TimeSpan fromNow);

        void CommitBatch(string transactionID);

        void UnMarkAll();
        void UnMarkAll(string q);
        bool ServiceIsDown();

        
        bool ResetConnection();



    }


}
