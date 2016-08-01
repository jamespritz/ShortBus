using ShortBus.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Configuration {
    public class EndpointConfigPersist<T> {

        private IPersist db;
        public EndpointConfigPersist(IPersist db) {

            this.db = db;

        }

        public T GetConfig() {

            T toReturn = default(T);


            PersistedMessage last = db.PeekNext("config");
            if (last != null) {

                string serialized = last.PayLoad;
                toReturn = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(serialized);

            }
         

            return toReturn;
        }

        public void UpdateConfig(T config) {

            string serialized = Newtonsoft.Json.JsonConvert.SerializeObject(config);
            string typeName = Util.Util.GetTypeName(typeof(T));
            PersistedMessage oldConfig = db.PeekNext("config");

            PersistedMessage newConfig = new PersistedMessage(serialized) {
                Distributed = null
                , MessageHandler = null
                , HandleRetryCount = 0
                , Headers = new Dictionary<string, string>()
                , MessageType = typeName
                , Ordinal = 0
                , Published = null
                , Publisher = null
                , SendRetryCount = 0
                , Sent = DateTime.UtcNow
                , Status = PersistedMessageStatusOptions.ReadyToProcess
                , Subscriber = null
                , Queue = "config"
            };
            db.Persist(new List<PersistedMessage>() { newConfig });
            //peek pulls latest anyway, so if this fails, no harm done
            if (oldConfig != null) {
                db.Mark(oldConfig.Id);
            }

        }

    }
}
