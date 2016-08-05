using ShortBus.Configuration;
using ShortBus.Publish;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ShortBus.Persistence {




    /// <summary>
    /// implementation must provide constructor that 
    /// </summary>
    public interface IPeristProvider {
        IPersist CreatePersist(IConfigure Configure);
        Task<IPersist> CreatePersistAsync(IConfigure Configure);
        Task<IPersist> CreatePersistAsync(IConfigure Configure, CancellationToken token);

    }

    /// <summary>
    /// Operations required to perist a message to local storage
    /// </summary>
    public interface IPersist {


        bool Persist(IEnumerable<PersistedMessage> messages);
        Task<bool> PersistAsync(IEnumerable<PersistedMessage> messages);
        Task<bool> PersistAsync(IEnumerable<PersistedMessage> messages, CancellationToken token);



        PersistedMessage PeekNext(string q);
        Task<PersistedMessage> PeekNextAsync(string q);
        Task<PersistedMessage> PeekNextAsync(string q, CancellationToken token);

        PersistedMessage PeekAndMarkNext(string q, PersistedMessageStatusOptions mark);
        Task<PersistedMessage> PeekAndMarkNextAsync(string q, PersistedMessageStatusOptions mark);
        Task<PersistedMessage> PeekAndMarkNextAsync(string q, PersistedMessageStatusOptions mark, CancellationToken token);

        PersistedMessage Mark(Guid id, PersistedMessageStatusOptions mark);
        Task<PersistedMessage> MarkAsync(Guid id, PersistedMessageStatusOptions mark);
        Task<PersistedMessage> MarkAsync(Guid id, PersistedMessageStatusOptions mark, CancellationToken token);

        
        PersistedMessage Pop(Guid Id);
        Task<PersistedMessage> PopAsync(Guid Id);
        Task<PersistedMessage> PopAsync(Guid Id, CancellationToken token);

        PersistedMessage Reschedule(Guid id, TimeSpan fromNow);
        Task<PersistedMessage> RescheduleAsync(Guid id, TimeSpan fromNow);
        Task<PersistedMessage> RescheduleAsync(Guid id, TimeSpan fromNow, CancellationToken token);

        bool CommitBatch(string transactionID);
        Task<bool> CommitBatchAsync(string transactionID);
        Task<bool> CommitBatchAsync(string transactionID, CancellationToken token);

        bool ToggleMarkAll(PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark);
        Task<bool> ToggleMarkAllAsync(PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark);
        Task<bool> ToggleMarkAllAsync(PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark, CancellationToken token);

        bool ToggleMarkAll(string q, PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark);
        Task<bool> ToggleMarkAllAsync(string q, PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark);
        Task<bool> ToggleMarkAllAsync(string q, PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark, CancellationToken token);

        bool ServiceIsDown();

        
        bool ResetConnection();
        Task<bool> ResetConnectionAsync();
        Task<bool> ResetConnectionAsync(CancellationToken token);



    }


}
