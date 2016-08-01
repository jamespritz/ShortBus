using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using Xunit;
using ShortBus.Default;
using ShortBus.Persistence;
using MongoDB.Driver;
using MongoDB.Bson;

namespace Tests.Persistence {



    public class MongoPersistTest {

        [Fact()]
        public void DBExists() {

            MongoPersist toTest = new MongoPersist(new MongoPersistSettings() { Collection = "test", DB = "test", ConnectionString = "" });
            MongoProviderMockHelper helper = new MongoProviderMockHelper();

            helper.PrimeDatabase(new List<BsonDocument>() { new BsonDocument("name", "test") });


            toTest.MongoProvider = helper.MockProvider.Object;
            IPersist asI = (IPersist)toTest;
            Assert.True(asI.DBExists);

        }

        [Fact()]
        public void DBDoesNotExist() {

            MongoPersist toTest = new MongoPersist(new MongoPersistSettings() { Collection = "test", DB = "test", ConnectionString = "" });
            MongoProviderMockHelper helper = new MongoProviderMockHelper();

            //wire up cursor to return no databases
            bool doMoveNext = true;
            Func<bool> cursorMoveNext = () => {
                if (doMoveNext) {
                    doMoveNext = false;
                    return true;
                }
                return false;
            };

            helper.PrimeDatabase(new List<BsonDocument>() { });

            toTest.MongoProvider = helper.MockProvider.Object;
            IPersist asI = (IPersist)toTest;
            Assert.False(asI.DBExists);

        }

        [Fact()]
        public void GetCollectionCreatesIfNotExists() {



            MongoPersist toTest = new MongoPersist(
                new MongoPersistSettings() {
                    Collection = "newCollection"
                    , ConnectionString = ""
                    , DB = "test"
                });

            IPersist asI = (IPersist)toTest;

            MongoProviderMockHelper helper = new MongoProviderMockHelper();
            
            helper.PrimeDatabase(new List<BsonDocument>() { new BsonDocument("name", "test") });
            //need to prime an empty collection so that the cursor works
            helper.PrimeCollection(new List<BsonDocument>() { });


            

            toTest.MongoProvider = helper.MockProvider.Object;

            //this method should call create database
            IMongoCollection<PersistedMessage> result = toTest.GetCollection();
            
            
            helper.MockDatabase.Verify(m => m.CreateCollection(It.IsAny<string>()
                , It.IsAny<CreateCollectionOptions>() 
                , It.IsAny<System.Threading.CancellationToken>()));
            
        }

    }
}
