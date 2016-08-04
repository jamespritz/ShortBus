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

                Headers = new Dictionary<string, string>()
                , MessageType = typeName
                , Ordinal = 0
                , DateStamp = DateTime.UtcNow
                , Status = PersistedMessageStatusOptions.ReadyToProcess
                , Queue = "config"
            };
            newConfig.Routes.Add(new Route() {
                EndPointName = "config",
                EndPointType = Publish.EndPointTypeOptions.Handler
                , Routed = DateTime.UtcNow
            });

            db.Persist(new List<PersistedMessage>() { newConfig });
            //peek pulls latest anyway, so if this fails, no harm done
            if (oldConfig != null) {
                db.Mark(oldConfig.Id, PersistedMessageStatusOptions.Marked);
            }

        }

    }
}
