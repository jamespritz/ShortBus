using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB;
using ShortBus.Default;
using Moq;
using MongoDB.Driver;
using MongoDB.Bson;
using ShortBus.Persistence;

namespace Tests.Persistence {

    public class FakeDB {
        private Dictionary<string, IMongoCollection<PersistedMessage>> collections;
        public FakeDB() {
            this.collections = new Dictionary<string, IMongoCollection<PersistedMessage>>();
        }

        public Dictionary<string, IMongoCollection<PersistedMessage>> Collections { get {
                return this.collections;
            }
            set {
                this.collections = value;
            }
        }

        static bool dbCursor = false;
        public Func<bool> NextDB() {
            dbCursor = true;
            return () => {
                if (dbCursor) {
                    dbCursor = false;
                    return true;

                } else {
                    return false;
                }
            };
        }

        static bool collectionCursor = false;
        public Func<bool> NextCollection() {
            collectionCursor = true;
            return () => {
                if (collectionCursor) {
                    collectionCursor = false;
                    return true;

                } else {
                    return false;
                }
            };
        }

        public List<BsonDocument> CollectionDictionary() {
            return (from z in this.collections select new BsonDocument("name", z.Key)).ToList();
        }

    }
    public class FakeCollection<T> {
        public List<T> Items { get; set; }
    }

    public class MongoProviderMockHelper {

        public Mock<IMongoProvider> MockProvider { get; set; }
        public Mock<IMongoClient> MockClient { get; set; }
        public Mock<IMongoDatabase> MockDatabase { get; set; }

        public FakeDB Db { get; set; }
        

        public MongoProviderMockHelper() {
            
            this.MockProvider = new Mock<IMongoProvider>();
            this.MockClient = new Mock<IMongoClient>();
            this.MockDatabase = new Mock<IMongoDatabase>();
            this.Db = new FakeDB();

            MockClient.Setup(m => m.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>())).Returns(MockDatabase.Object);
            MockProvider.Setup(m => m.GetClient()).Returns(MockClient.Object);

        }

        public void PrimeDatabase(List<BsonDocument> dbs) {
 

            Mock<IAsyncCursor<BsonDocument>> mCursor = new Mock<IAsyncCursor<BsonDocument>>();
            //need to ensure movenext returns true only once
            mCursor.Setup(m => m.MoveNext(It.IsAny<System.Threading.CancellationToken>())). Returns(this.Db.NextDB());
            //return empty set to mimic database does not exist
            mCursor.Setup(m => m.Current).Returns(dbs);
            this.MockClient.Setup(m => m.ListDatabases(It.IsAny<System.Threading.CancellationToken>())).Returns(mCursor.Object);

        }

        public void PrimeCollection(List<BsonDocument> collections) {
 



            

            MockDatabase.Setup(g => g.GetCollection<PersistedMessage>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
                .Returns((string  s, MongoCollectionSettings z) => {
                    var toReturn = this.Db.Collections[s];
                    if (toReturn != null) {
                        return toReturn;
                    } else return null;
                });

            MockDatabase.Setup(g => g.CreateCollection(It.IsAny<string>()
                , It.IsAny<CreateCollectionOptions>()
                , It.IsAny<System.Threading.CancellationToken>())).Callback((string s, CreateCollectionOptions o, System.Threading.CancellationToken t) => {
                    //reprime collection so that this new collection is returned
                    //helper.PrimeCollection(new List<BsonDocument>() { new BsonDocument("name", "newCollection") });
                    Mock<IMongoCollection<PersistedMessage>> mCollection = new Mock<IMongoCollection<PersistedMessage>>();
                    Mock<IMongoIndexManager<PersistedMessage>> mIdxMgr = new Mock<IMongoIndexManager<PersistedMessage>>();
                    mCollection.Setup(g => g.Indexes).Returns(mIdxMgr.Object);

                    mIdxMgr.Setup(g => g.CreateMany(It.IsAny<IEnumerable<CreateIndexModel<PersistedMessage>>>()
                        , It.IsAny<System.Threading.CancellationToken>())).Returns(new string[] { });

                    this.Db.Collections.Add(s, mCollection.Object);
                });

            Mock<IAsyncCursor<BsonDocument>> mCursor = new Mock<IAsyncCursor<BsonDocument>>();
            //need to ensure movenext returns true only once
            mCursor.Setup(m => m.MoveNext(It.IsAny<System.Threading.CancellationToken>())).Returns(this.Db.NextCollection());
            //return empty set to mimic database does not exist
            mCursor.Setup(m => m.Current).Returns(this.Db.CollectionDictionary());

            MockDatabase.Setup(g => g.ListCollections(It.IsAny<ListCollectionsOptions>(), It.IsAny<System.Threading.CancellationToken>()))
                .Returns(mCursor.Object);
            
            //mock db.listcollections to return

        }
    }
}
