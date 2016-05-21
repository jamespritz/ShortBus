using ShortBus.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using MongoDB.Driver;
using MongoDB.Bson;

namespace ShortBus.Default {


    public class MongoPersistSettings {
        public string ConnectionString { get;set; }
        public string DB { get;set; }
        public string Collection { get; set; }

    }


    public class MongoPersist : IPersist {

        private MongoPersistSettings settings = null;
        private bool serviceDown = false;

        private IMongoClient client = null;
        private IMongoDatabase db = null;
       

        public MongoPersist(MongoPersistSettings settings ) {
            this.settings = settings;
        }

        private bool collectionExists = false;
        private IMongoCollection<PersistedMessage> GetCollection() {

            IMongoCollection<PersistedMessage> collection = null;
            try { 

                if (client == null) {
                    client = new MongoClient(this.settings.ConnectionString);
                }
                if (db == null) {
                    db = client.GetDatabase(this.settings.DB);
                    
                }

                if (!collectionExists && !((IPersist)this).CollectionExists) {
                    db.CreateCollection(this.settings.Collection);
                    collection = db.GetCollection<PersistedMessage>(this.settings.Collection);

                    var keys = Builders<PersistedMessage>.IndexKeys;

                    var idxOrdinal = keys.Ascending(g => g.Ordinal);
                    var idxSend = keys.Ascending(g => g.Sent);
                    CreateIndexModel<PersistedMessage> idxModelOrdinal = new CreateIndexModel<PersistedMessage>(idxOrdinal);
                    CreateIndexModel<PersistedMessage> idxModelSend = new CreateIndexModel<PersistedMessage>(idxSend);

                    collection.Indexes.CreateMany(new CreateIndexModel<PersistedMessage>[] { idxModelOrdinal, idxModelSend });
                    this.collectionExists = true;

                }


                if (collection == null) {
                    collection = db.GetCollection<PersistedMessage>(this.settings.Collection);
                    collectionExists = true;
                }
            
            } catch (Exception e) {
                this.serviceDown = true;
                throw new ServiceEndpointDownException("Mongo Persist Service is Down", e);
            } 
            return collection;
        }

        //async Task IPersist.PersistAsync(PersistedMessage message) {
            
        //    message.Sent = DateTime.UtcNow;
        //    collection = this.GetCollection();

            
        //    await collection.InsertOneAsync(message);

        //}

        bool IPersist.DBExists {  get {

                bool found = false;
                if (client == null) {
                    client = new MongoClient(this.settings.ConnectionString);
                }


                using (var cursor = client.ListDatabases()) {
                    while (cursor.MoveNext()) {
                        foreach (BsonDocument i in cursor.Current) {
                            string name = i["name"].ToString();

                            if (name.Equals(this.settings.DB, StringComparison.OrdinalIgnoreCase)) {
                                found = true;
                                
                            }
                            
                        }
                    }
                }
            
                return found;
            }
        }

        bool IPersist.CollectionExists {
            get {
                bool found = false;

                if (client == null) {
                    client = new MongoClient(this.settings.ConnectionString);
                }
                if (db == null) {
                    db = client.GetDatabase(this.settings.DB);

                }
                using (var cursor = db.ListCollections()) {
                    while (cursor.MoveNext()) {
                        foreach (BsonDocument i in cursor.Current) {
                            string name = i["name"].ToString();

                            if (name.Equals(this.settings.Collection, StringComparison.OrdinalIgnoreCase)) {
                                found = true;

                            }

                        }
                    }
                }
                return found;
            }
        }

        void IPersist.Persist(IEnumerable<PersistedMessage> messages) {

            if (this.serviceDown) {
                throw new ServiceEndpointDownException("Mongo Persist Service is Down");
            }


            
            

            

            IMongoCollection<PersistedMessage> collection = this.GetCollection();

            try {
                foreach (PersistedMessage m in messages) {
                    m.Sent = DateTime.UtcNow;
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

                var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess), fBuilder.Eq(g => g.Queue, q));
                var update = Builders<PersistedMessage>.Update.Set(g => g.Status, PersistedMessageStatusOptions.Marked);
                var sort = Builders<PersistedMessage>.Sort.Ascending(e => e.Sent).Ascending(e => e.Ordinal);
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

                var qfilter = fBuilder.And(fBuilder.Eq(g => g.Status, PersistedMessageStatusOptions.ReadyToProcess), fBuilder.Eq(g => g.Queue, q));
                var sort = Builders<PersistedMessage>.Sort.Ascending(e => e.Sent).Ascending(e => e.Ordinal);
                var options = new FindOptions<PersistedMessage>() {  Sort = sort };

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
        //async Task<PersistedMessage> IPersist.PopAsync(Guid Id) {
        //    collection = this.GetCollection();
        //    var filter = Builders<PersistedMessage>.Filter.Eq(m => m.Id, Id);


        //    var pop = await collection.FindOneAndDeleteAsync(filter);

        //    return pop;

        //}
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
                    .Set(g => g.Sent, toSet)
                    .Inc(g => g.HandleRetryCount, 1);
                 

                    
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

            
            db = null;
            client = null;
  

            try {

                
                ((IPersist)this).Persist(new PersistedMessage[] { new PersistedMessage("Test") { Sent = DateTime.UtcNow, Publisher = "diag_test" } });
                PersistedMessage peeked = ((IPersist)this).PeekAndMarkNext("diag_test");
                PersistedMessage popped = ((IPersist)this).Pop(peeked.Id);
                
            } catch (Exception) {
                this.serviceDown = true;

            }
            return this.serviceDown;

        }
    }
}
