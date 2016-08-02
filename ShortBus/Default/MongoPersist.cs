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

        IPersist IPeristProvider.CreatePersist(IConfigure Configure) {


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



            if (this.CollectionExists(collectionName)) {

                toReturn = new MongoPersist(new MongoSettings() { ServerName = this.server, DataBaseName = this.dbName.DB, CollectionName = collectionName });
                EndpointConfigPersist<EndPointConfigBase> configReader = new EndpointConfigPersist<EndPointConfigBase>(toReturn);
                //get config and ensure its the same
                EndPointConfigBase config = configReader.GetConfig();

                if (config.ApplicationGUID != Configure.ApplicationGUID) {
                    throw new Exception("Collection exists but has already been configured for another process");
                }

            } else { //if not, we must create

                //create collection   
                IMongoDatabase db = this.mongoProvider.GetClient().GetDatabase(dbName.DB);             
                db.CreateCollection(collectionName);
                IMongoCollection<PersistedMessage> collection = db.GetCollection<PersistedMessage>(collectionName);

                collection = this.CreateIndexes(collection);

                toReturn = new MongoPersist(new MongoSettings() { ServerName = this.server, DataBaseName = this.dbName.DB, CollectionName = collectionName });
                EndpointConfigPersist<EndPointConfigBase> configReader = new EndpointConfigPersist<EndPointConfigBase>(toReturn);
                EndPointConfigBase config = new EndPointConfigBase() { ApplicationGUID = Configure.ApplicationGUID, ApplicationName = Configure.ApplicationName, Version = "1.0" };
                configReader.UpdateConfig(config);
            }

            return toReturn;

        }


        public virtual IMongoCollection<PersistedMessage> CreateIndexes(IMongoCollection<PersistedMessage> collection) {
            var keys = Builders<PersistedMessage>.IndexKeys;

            var idxOrdinal = keys.Ascending(g => g.Ordinal);
            var idxSend = keys.Ascending(g => g.DateStamp);
            CreateIndexModel<PersistedMessage> idxModelOrdinal = new CreateIndexModel<PersistedMessage>(idxOrdinal);
            CreateIndexModel<PersistedMessage> idxModelSend = new CreateIndexModel<PersistedMessage>(idxSend);

            collection.Indexes.CreateMany(new CreateIndexModel<PersistedMessage>[] { idxModelOrdinal, idxModelSend });
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



        bool CollectionExists(string collectionName) {

            bool collectionExists = false;



            IMongoDatabase db = this.mongoProvider.GetClient().GetDatabase(this.dbName.DB);


            using (var cursor = db.ListCollections()) {
                while (cursor.MoveNext()) {
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




        public virtual IMongoCollection<PersistedMessage> GetCollection() {

            IMongoCollection<PersistedMessage> collection = null;
            try {

                IMongoDatabase db = this.GetDatabase();

                collection = db.GetCollection<PersistedMessage>(this.settings.CollectionName);



            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }
            return collection;
        }



        void IPersist.Persist(IEnumerable<PersistedMessage> messages) {

            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }







            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                foreach (PersistedMessage m in messages) {
                    m.DateStamp = DateTime.UtcNow;
                }

                collection.InsertMany(messages);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }
        }
        //async Task<PersistedMessage> IPersist.GetNextAsync() {

        //    collection = this.GetCollection();

        //    var qfilter = Builders<PersistedMessage>.Filter.Ne("Status", "1");
        //    var pop = await collection.Find(qfilter).SortByDescending(e => e.Sent).Limit(1).FirstOrDefaultAsync();
        //    if (pop != null) { 

        //        var update = Builders<PersistedMessage>.Update.Set(g => g.Status, 1);
        //        var ufilter = Builders<PersistedMessage>.Filter.Eq(g => g.Id, pop.Id);
        //        pop = await collection.FindOneAndUpdateAsync(ufilter, update);



        //    }
        //    return pop;

        //}

        PersistedMessage IPersist.PeekAndMarkNext(string q) {

            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();
            PersistedMessage pop = null;

            try {
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess), fBuilder.Eq(g => g.Queue, q), fBuilder.Lte(g => g.DateStamp, DateTime.UtcNow));
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, PersistedMessageStatusOptions.Marked);
                var sort = Builders<PersistedMessage>.Sort.Ascending(e => e.DateStamp).Ascending(e => e.Ordinal);
                var options = new FindOneAndUpdateOptions<PersistedMessage>() { ReturnDocument = ReturnDocument.After, Sort = sort };

                pop = collection.FindOneAndUpdate(qfilter, update, options);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;

        }

        void IPersist.UnMarkAll() {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                var qfilter = fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.Marked);
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess);

                collection.UpdateMany(qfilter, update);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

        }

        void IPersist.CommitBatch(string transactionID) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();



            try {
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.Uncommitted), fBuilder.Eq(g => g.TransactionID, transactionID));
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess);

                collection.UpdateMany(qfilter, update);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }
        }

        void IPersist.UnMarkAll(string q) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();



            try {
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.Marked), fBuilder.Eq(g => g.Queue, q));
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess);

                collection.UpdateMany(qfilter, update);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

        }


        PersistedMessage IPersist.PeekNext(string q) {

            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            IMongoCollection<PersistedMessage> collection = this.GetCollection();
            PersistedMessage pop = null;

            try {
                FilterDefinitionBuilder<PersistedMessage> fBuilder = Builders<PersistedMessage>.Filter;

                var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess), fBuilder.Eq(g => g.Queue, q), fBuilder.Lte(g => g.DateStamp, DateTime.UtcNow));
                var sort = Builders<PersistedMessage>.Sort.Ascending(e => e.DateStamp).Ascending(e => e.Ordinal);
                var options = new FindOptions<PersistedMessage>() { Sort = sort };

                Task<IAsyncCursor<PersistedMessage>> t = collection.FindAsync(qfilter, options);
                t.Wait();
                IAsyncCursor<PersistedMessage> cursor = t.Result;
                while (cursor.MoveNext()) {
                    if (cursor.Current.Count() > 0) {
                        pop = cursor.Current.FirstOrDefault();
                    }
                }
                return pop;


            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;

        }

        PersistedMessage IPersist.Pop(Guid Id) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            PersistedMessage pop = null;
            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                var filter = Builders<PersistedMessage>.Filter.Eq(m => m.Id, Id);


                pop = collection.FindOneAndDelete(filter);
            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;

        }

        PersistedMessage IPersist.Mark(Guid Id) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            PersistedMessage pop = null;
            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, PersistedMessageStatusOptions.Marked);
                var filter = Builders<PersistedMessage>.Filter.Eq(m => m.Id, Id);


                collection.UpdateOne(filter, update);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;

        }

        PersistedMessage IPersist.Processing(Guid Id) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            PersistedMessage pop = null;
            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, PersistedMessageStatusOptions.Processing);
                var filter = Builders<PersistedMessage>.Filter.Eq(m => m.Id, Id);


                collection.UpdateOne(filter, update);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;

        }

        PersistedMessage IPersist.Reschedule(Guid Id, TimeSpan fromnNow) {
            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }

            PersistedMessage pop = null;
            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                DateTime toSet = DateTime.UtcNow.Add(fromnNow);
                var update = Builders<PersistedMessage>.Update
                    .Set(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess)
                    .Set(g => g.DateStamp, toSet)
                    .Inc(g => g.RetryCount, 1);



                var filter = Builders<PersistedMessage>.Filter.Eq(m => m.Id, Id);


                var r = collection.UpdateOne(filter, update);

            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            }

            return pop;

        }

        bool IPersist.ServiceIsDown() {
            return this.serviceDown;
        }

        bool IPersist.ResetConnection() {
            this.serviceDown = false;


            dbInstance = null;
        


            try {


                ((IPersist)this).Persist(new PersistedMessage[] { new PersistedMessage("Test") { Queue = "diag_test" , DateStamp = DateTime.UtcNow } });
                PersistedMessage peeked = ((IPersist)this).PeekAndMarkNext("diag_test");
                PersistedMessage popped = ((IPersist)this).Pop(peeked.Id);

            } catch (Exception) {
                this.serviceDown = true;

            }
            return this.serviceDown;

        }
    }
    

}
