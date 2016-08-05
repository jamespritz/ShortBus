using ShortBus.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ShortBus.Configuration {
    public class EndpointConfigPersist<T> {

        private IPersist db;
        public EndpointConfigPersist(IPersist db) {

            this.db = db;

        }

        public async Task<T> GetConfigAsync(CancellationToken token) {

            T toReturn = default(T);


            PersistedMessage last = await db.PeekNextAsync("config", token);
            if (last != null) {

                string serialized = last.PayLoad;
                toReturn = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(serialized);

            }


            return toReturn;
        }

        public T GetConfig() {

            try {
                CancellationTokenSource cts = new CancellationTokenSource();

                Task<T> result = this.GetConfigAsync(cts.Token);
                result.Wait();
                return result.Result;
            } catch {
                throw;
            }
        }

        public bool UpdateConfig(T config) {
            CancellationTokenSource cts = new CancellationTokenSource();
            Task<bool> result = this.UpdateConfigAsync(config, cts.Token);
            result.Wait();
            return result.Result;
        }

        public async Task<bool> UpdateConfigAsync(T config, CancellationToken token) {

            bool toReturn = false;

            string serialized = Newtonsoft.Json.JsonConvert.SerializeObject(config);
            string typeName = Util.Util.GetTypeName(typeof(T));
            PersistedMessage oldConfig = await db.PeekNextAsync("config", token);

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

            toReturn = await db.PersistAsync(new List<PersistedMessage>() { newConfig }, token);
            //peek pulls latest anyway, so if this fails, no harm done
            if (oldConfig != null) {
                await db.MarkAsync(oldConfig.Id, PersistedMessageStatusOptions.Marked, token);
            }
            return toReturn;

        }

    }
}
