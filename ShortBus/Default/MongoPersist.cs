using ShortBus.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using MongoDB.Driver;
using MongoDB.Bson;
using ShortBus.Configuration;
using ShortBus.Publish;
using System.Threading;

namespace ShortBus.Default {

    public interface IMongoProvider<T> {
        T GetClient();


    }

    public class MongoProvider : IMongoProvider<IMongoClient> {

        private string connx;
        public MongoProvider(string connx) {
            this.connx = connx;
        }

        private IMongoClient clientInstance = null;
        public IMongoClient GetClient() {

            if (clientInstance == null) {
                clientInstance = new MongoClient(this.connx);
            }
            return clientInstance;
        }

    }


    public struct MongoDataBaseName {

        public const string CREATE = "CREATE";

        private string db;
        internal MongoDataBaseName(string db) {
            this.db = db;
        }

        //i'm being lazy b/c i had overriding equals
        public string DB {  get {
                return this.db;
            }
        }

        public static implicit operator MongoDataBaseName(string db) {
            return new MongoDataBaseName(db);
        }

        public static MongoDataBaseName UseExisting(string db) {
            return new MongoDataBaseName(db);
        }
        public static MongoDataBaseName Create() {
            return new MongoDataBaseName(CREATE);
        }

        //public static bool operator ==(MongoDataBaseName first, string second) {
        //    return first.db.Equals(second);
        //}
        //public static bool operator !=(MongoDataBaseName first, string second) {
        //    return !first.db.Equals(second);
        //}
        //public override bool Equals(object obj) {
            
        //}
    }

    public class MongoPersistProvider : IPeristProvider {

        private string server = string.Empty;
        private MongoDataBaseName dbName = MongoDataBaseName.Create();

        private IMongoProvider<IMongoClient> mongoProvider = null;

        //provided for injecting a mock while testing
        public IMongoProvider<IMongoClient> MongoProvider {
            get {
                return this.mongoProvider;
            }
            set {
                this.mongoProvider = value;
            }
        }

        public MongoPersistProvider(string server, MongoDataBaseName dbName) {
            this.server = server;
            this.dbName = dbName;
        }

        //private string GetCollectionType(EndPointTypeOptions collectionType) {
        //    string toReturn = string.Empty;
        //    switch(collectionType) {
        //        case EndPointTypeOptions.Publisher:
        //            toReturn = "Publish";
        //            break;
        //        case EndPointTypeOptions.Source:
        //            toReturn = "Source";
        //            break;
        //        case EndPointTypeOptions.Subscriber:
        //            toReturn = "Subscribe";
        //            break;

        //    }
        //    return toReturn;
        //}



        public virtual async Task<IMongoCollection<PersistedMessage>> CreateIndexesAsync(IMongoCollection<PersistedMessage> collection, CancellationToken token) {
            var keys = Builders<PersistedMessage>.IndexKeys;

            

            var idxOrdinal = keys.Ascending(g => g.Ordinal);
            var idxSend = keys.Ascending(g => g.DateStamp);
            CreateIndexModel<PersistedMessage> idxModelOrdinal = new CreateIndexModel<PersistedMessage>(idxOrdinal);
            CreateIndexModel<PersistedMessage> idxModelSend = new CreateIndexModel<PersistedMessage>(idxSend);

            await collection.Indexes.CreateManyAsync(new CreateIndexModel<PersistedMessage>[] { idxModelOrdinal, idxModelSend }, token);
            return collection;

        }

        public bool DBExists() {


            bool found = false;


            IMongoClient client = this.MongoProvider.GetClient();

            using (var cursor = client.ListDatabases()) {
                while (cursor.MoveNext()) {
                    foreach (BsonDocument i in cursor.Current) {
                        string name = i["name"].ToString();

                        if (name.Equals(dbName.DB, StringComparison.OrdinalIgnoreCase)) {
                            found = true;

                        }

                    }
                }
            }

            return found;

        }



        public async Task<bool> CollectionExistsAsync(string collectionName, CancellationToken token) {

            bool collectionExists = false;

            
            IMongoDatabase db = this.mongoProvider.GetClient().GetDatabase(this.dbName.DB);

            using (var cursor = await db.ListCollectionsAsync(new ListCollectionsOptions() { Filter = Builders<BsonDocument>.Filter.Eq("name", collectionName) }, token)) {
                while (await cursor.MoveNextAsync()) {
                    foreach (BsonDocument i in cursor.Current) {
                        string name = i["name"].ToString();

                        if (name.Equals(collectionName, StringComparison.OrdinalIgnoreCase)) {
                            collectionExists = true;

                        }

                    }
                }

                return collectionExists;

            }
        }

        async Task<IPersist> IPeristProvider.CreatePersistAsync(IConfigure Configure) {

            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPeristProvider)this).CreatePersistAsync(Configure, cts.Token);

        }

        IPersist IPeristProvider.CreatePersist(IConfigure Configure) {


            Task<IPersist> toReturn = ((IPeristProvider)this).CreatePersistAsync(Configure);
            toReturn.Wait();
            return toReturn.Result;

        }


        async Task<IPersist> IPeristProvider.CreatePersistAsync(IConfigure Configure, CancellationToken token) {

            string collectionBase = string.Empty;

            MongoPersist toReturn = null;
            string collectionName = string.Empty;

            bool CreateDatabase = this.dbName.DB == MongoDataBaseName.CREATE;
            //if database provided, our collection names must be unique, so we factor in applicationname
            if (!CreateDatabase) {

                collectionName = string.Format("{0}_{1}", Configure.ApplicationName, "messages");

            } else { //if we are creating database, our collectionname can be generic
                this.dbName = Configure.ApplicationName;
                collectionName = "messages";
            }




            // TODO: test if database exists and that it is not configured for another collection

            this.mongoProvider = new MongoProvider(server);

            ////if not createdatabase, then the one passed in must exist
            //if (!CreateDatabase) {
            //    if (!this.DBExists()) {
            //        throw new Exception(string.Format("{0} does not exists", this.dbName.DB));
            //    }
            //}

            bool exists = await this.CollectionExistsAsync(collectionName, token);

            if (exists) {

                toReturn = new MongoPersist(new MongoSettings() { ServerName = this.server, DataBaseName = this.dbName.DB, CollectionName = collectionName });
                EndpointConfigPersist<EndPointConfigBase> configReader = new EndpointConfigPersist<EndPointConfigBase>(toReturn);
                //get config and ensure its the same
                EndPointConfigBase config = await configReader.GetConfigAsync(token);

                if (config.ApplicationGUID != Configure.ApplicationGUID) {
                    throw new Exception("Collection exists but has already been configured for another process");
                }

            } else { //if not, we must create

                //create collection   
                IMongoDatabase db = this.mongoProvider.GetClient().GetDatabase(dbName.DB);
                await db.CreateCollectionAsync(collectionName, null, token);
                IMongoCollection<PersistedMessage> collection = db.GetCollection<PersistedMessage>(collectionName);

                collection = await this.CreateIndexesAsync(collection, token);

                toReturn = new MongoPersist(new MongoSettings() { ServerName = this.server, DataBaseName = this.dbName.DB, CollectionName = collectionName });
                EndpointConfigPersist<EndPointConfigBase> configReader = new EndpointConfigPersist<EndPointConfigBase>(toReturn);
                EndPointConfigBase config = new EndPointConfigBase() { ApplicationGUID = Configure.ApplicationGUID, ApplicationName = Configure.ApplicationName, Version = "1.0" };
                await configReader.UpdateConfigAsync(config, token);
            }

            return toReturn;
        }

    }

    public struct MongoSettings {
        public string ServerName { get; set; }
        public string DataBaseName { get; set; }
        public string CollectionName { get; set; }

    }

    public class MongoPersist : IPersist {


        private bool serviceDown = false;


        private IMongoDatabase dbInstance = null;
        private MongoSettings settings;



        public MongoPersist(MongoSettings settings) {
            this.settings = settings;
            this.mongoProvider = new MongoProvider(settings.ServerName);
        }


        private IMongoProvider<IMongoClient> mongoProvider = null;

        //provided for injecting a mock while testing
        public IMongoProvider<IMongoClient> MongoProvider {
            get {
                return this.mongoProvider;
            }
            set { this.mongoProvider = value; }
        }

        public IMongoDatabase GetDatabase() {
            if (this.dbInstance == null) {
                this.dbInstance = MongoProvider.GetClient().GetDatabase(settings.DataBaseName);
            }
            return this.dbInstance;
        }



        IMongoCollection<PersistedMessage> myCollection = null;
        public virtual IMongoCollection<PersistedMessage> GetCollection() {

            
            try {
                if (myCollection == null) {

                    IMongoDatabase db = this.GetDatabase();

                    myCollection = db.GetCollection<PersistedMessage>(this.settings.CollectionName);

                }

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return myCollection;
        }



        bool IPersist.Persist(IEnumerable<PersistedMessage> messages){
            Task<bool> t = ((IPersist)this).PersistAsync(messages);
            t.Wait();
            return t.Result;
        }

        async Task<bool> IPersist.PersistAsync(IEnumerable<PersistedMessage> messages) {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).PersistAsync(messages, cts.Token);


        }

        async Task<bool> IPersist.PersistAsync(IEnumerable<PersistedMessage> messages, CancellationToken token) {

            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }


            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                token.ThrowIfCancellationRequested();
                foreach (PersistedMessage m in messages) {
                    m.DateStamp = DateTime.UtcNow;
                }

                await collection.InsertManyAsync(messages, null, token);
                return true;

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }
        }

        bool IPersist.ServiceIsDown() {
            return this.serviceDown;
        }

        bool IPersist.ResetConnection() {
            Task<bool> t = ((IPersist)this).ResetConnectionAsync();
            t.Wait();
            return t.Result;
        }

        async Task<bool> IPersist.ResetConnectionAsync() {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).ResetConnectionAsync(cts.Token);
        }

        async Task<bool> IPersist.ResetConnectionAsync(CancellationToken token) {
            this.serviceDown = false;

            dbInstance = null;

            try {


                await ((IPersist)this).PersistAsync(new PersistedMessage[] { new PersistedMessage("Test") { Queue = "diag_test", DateStamp = DateTime.UtcNow } }, token);
                PersistedMessage peeked = await ((IPersist)this).PeekAndMarkNextAsync("diag_test", PersistedMessageStatusOptions.Marked, token);
                PersistedMessage popped = await ((IPersist)this).PopAsync(peeked.Id, token);

            } catch (Exception) {
                this.serviceDown = true;

            }
            return this.serviceDown;
        }

        PersistedMessage IPersist.PeekNext(string q) {

            Task<PersistedMessage> t = ((IPersist)this).PeekNextAsync(q);
            t.Wait();
            return t.Result;
        }

        async Task<PersistedMessage> IPersist.PeekNextAsync(string q) {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).PeekNextAsync(q, cts.Token);
        }

        async Task<PersistedMessage> IPersist.PeekNextAsync(string q, CancellationToken token) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();
            PersistedMessage pop = null;

            try {
                token.ThrowIfCancellationRequested();
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                //var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess), fBuilder.Eq(g => g.Queue, q), fBuilder.Lte(g => g.DateStamp, DateTime.UtcNow));
                var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess), fBuilder.Eq(g => g.Queue, q));
                var sort = Builders<PersistedMessage>.Sort.Ascending(e => e.DateStamp).Ascending(e => e.Ordinal);
                var options = new FindOptions<PersistedMessage>() { Sort = sort };



                using (IAsyncCursor<PersistedMessage> cursor = await collection.FindAsync(qfilter, options, token)) {
                    while (await cursor.MoveNextAsync(token)) {
                        if (cursor.Current.Count() > 0) {
                            pop = cursor.Current.FirstOrDefault();
                        }
                    }
                }
                return pop;


            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

        }

        PersistedMessage IPersist.PeekAndMarkNext(string q, PersistedMessageStatusOptions mark) {

            Task<PersistedMessage> t = ((IPersist)this).PeekAndMarkNextAsync(q, mark);
            t.Wait();
            return t.Result;

        }

        async Task<PersistedMessage> IPersist.PeekAndMarkNextAsync(string q, PersistedMessageStatusOptions mark) {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).PeekAndMarkNextAsync(q, mark, cts.Token);
        }

        async Task<PersistedMessage> IPersist.PeekAndMarkNextAsync(string q, PersistedMessageStatusOptions mark, CancellationToken token) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();
            PersistedMessage pop = null;

            try {
                token.ThrowIfCancellationRequested();
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess), fBuilder.Eq(g => g.Queue, q), fBuilder.Lte(g => g.DateStamp, DateTime.UtcNow));
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, mark);
                var sort = Builders<PersistedMessage>.Sort.Ascending(e => e.DateStamp).Ascending(e => e.Ordinal);
                var options = new FindOneAndUpdateOptions<PersistedMessage>() { ReturnDocument = ReturnDocument.After, Sort = sort };

                pop = await collection.FindOneAndUpdateAsync(qfilter, update, options, token);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;
        }

        PersistedMessage IPersist.PeekAndMark(string q, PersistedMessageStatusOptions mark, Guid messageId) {

            Task<PersistedMessage> t = ((IPersist)this).PeekAndMarkAsync(q, mark, messageId);
            t.Wait();
            return t.Result;

        }

        async Task<PersistedMessage> IPersist.PeekAndMarkAsync(string q, PersistedMessageStatusOptions mark, Guid messageId) {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).PeekAndMarkAsync(q, mark, cts.Token, messageId);
        }

        async Task<PersistedMessage> IPersist.PeekAndMarkAsync(string q, PersistedMessageStatusOptions mark, CancellationToken token, Guid messageId) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();
            PersistedMessage pop = null;

            try {
                token.ThrowIfCancellationRequested();
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                //var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess), fBuilder.Eq(g => g.Queue, q), fBuilder.Lte(g => g.DateStamp, DateTime.UtcNow));
                var qfilter = fBuilder.Eq(g => g.Id, messageId);
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, mark);
                //var sort = Builders<PersistedMessage>.Sort.Ascending(e => e.DateStamp).Ascending(e => e.Ordinal);
                var options = new FindOneAndUpdateOptions<PersistedMessage>() { ReturnDocument = ReturnDocument.After };

                pop = await collection.FindOneAndUpdateAsync(qfilter, update, options, token);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;
        }


        async Task<PersistedMessage> IPersist.MarkAsync(Guid id, PersistedMessageStatusOptions mark) {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).MarkAsync(id, mark, cts.Token);
        }

        PersistedMessage IPersist.Mark(Guid Id, PersistedMessageStatusOptions mark) {
            Task<PersistedMessage> t = ((IPersist)this).MarkAsync(Id, mark);
            t.Wait();
            return t.Result;
        }

        async Task<PersistedMessage> IPersist.MarkAsync(Guid Id, PersistedMessageStatusOptions mark, CancellationToken token) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            PersistedMessage pop = null;
            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                token.ThrowIfCancellationRequested();
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, mark);
                var filter = Builders<PersistedMessage>.Filter.Eq(m => m.Id, Id);


                await collection.UpdateOneAsync(filter, update, null, token);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;
        }

        PersistedMessage IPersist.Pop(Guid Id) {
            Task<PersistedMessage> t = ((IPersist)this).PopAsync(Id);
            t.Wait();
            return t.Result;

        }

        async Task<PersistedMessage> IPersist.PopAsync(Guid Id) {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).PopAsync(Id, cts.Token);
        }

        async Task<PersistedMessage> IPersist.PopAsync(Guid Id, CancellationToken token) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            PersistedMessage pop = null;
            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                token.ThrowIfCancellationRequested();
                var filter = Builders<PersistedMessage>.Filter.Eq(m => m.Id, Id);


                pop = await collection.FindOneAndDeleteAsync(filter, null, token);
            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;
        }

        PersistedMessage IPersist.Reschedule(Guid Id, TimeSpan fromNow) {
            Task<PersistedMessage> t = ((IPersist)this).RescheduleAsync(Id, fromNow);
            t.Wait();
            return t.Result;

        }

        async Task<PersistedMessage> IPersist.RescheduleAsync(Guid id, TimeSpan fromNow) {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).RescheduleAsync(id, fromNow, cts.Token);
        }

        async Task<PersistedMessage> IPersist.RescheduleAsync(Guid Id, TimeSpan fromNow, CancellationToken token) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            PersistedMessage pop = null;
            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                token.ThrowIfCancellationRequested();
                DateTime toSet = DateTime.UtcNow.Add(fromNow);
                var update = Builders<PersistedMessage>.Update
                    .Set(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess)
                    .Set(g => g.DateStamp, toSet)
                    .Inc(g => g.RetryCount, 1);



                var filter = Builders<PersistedMessage>.Filter.Eq(m => m.Id, Id);


                pop = await collection.FindOneAndUpdateAsync(filter, update, null, token);
                
                

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;
        }

        bool IPersist.CommitBatch(string transactionID) {

            Task<bool> t = ((IPersist)this).CommitBatchAsync(transactionID);
            t.Wait();
            return t.Result;

        }

        async Task<bool> IPersist.CommitBatchAsync(string transactionID) {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).CommitBatchAsync(transactionID, cts.Token);
        }

        async Task<bool> IPersist.CommitBatchAsync(string transactionID, CancellationToken token) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();



            try {
                token.ThrowIfCancellationRequested();
                
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.Uncommitted), fBuilder.Eq(g => g.TransactionID, transactionID));
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess);

                await collection.UpdateManyAsync(qfilter, update, null, token);

                return true;

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }
        }

        bool IPersist.ToggleMarkAll(PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark) {
            Task<bool> t = ((IPersist)this).ToggleMarkAllAsync(newMark, oldMark);
            t.Wait();
          
            return t.Result;
        }

        async Task<bool> IPersist.ToggleMarkAllAsync(PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark) {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).ToggleMarkAllAsync(newMark, oldMark, cts.Token);
        }

        async Task<bool> IPersist.ToggleMarkAllAsync(PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark, CancellationToken token) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                token.ThrowIfCancellationRequested();
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                var qfilter = fBuilder.Eq(g => g.Status, oldMark);
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, newMark);

                await collection.UpdateManyAsync(qfilter, update, null, token);

                return true;

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }
        }

        bool IPersist.ToggleMarkAll(string q, PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark) {
            Task<bool> t = ((IPersist)this).ToggleMarkAllAsync(q, newMark, oldMark);
            t.Wait();
            return t.Result;
        }

        async Task<bool> IPersist.ToggleMarkAllAsync(string q, PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark) {
            CancellationTokenSource cts = new CancellationTokenSource();
            return await ((IPersist)this).ToggleMarkAllAsync(q, newMark, oldMark, cts.Token);
        }

        async Task<bool> IPersist.ToggleMarkAllAsync(string q, PersistedMessageStatusOptions newMark, PersistedMessageStatusOptions oldMark, CancellationToken token) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();



            try {
                token.ThrowIfCancellationRequested();
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, oldMark), fBuilder.Eq(g => g.Queue, q));
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, newMark);

                await collection.UpdateManyAsync(qfilter, update, null, token);

                return true;

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }
        }
    }
    

}
