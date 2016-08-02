

using ShortBus.Default;

using ShortBus.Persistence;
using ShortBus.Publish;
using ShortBus.Subscriber;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Configuration {



    public interface IConfigure {


        string ApplicationName { get; }

        string ApplicationGUID { get; }

        IConfigure SetApplicationName(string appName);
        IConfigure SetApplicationGUID(string appGUID);

        IConfigure PersistTo(IPeristProvider provider);

        IConfigure MyEndPoint(EndPoint myEndPoint);

        IConfigure MaxThreads(int threadCount);

        IConfigure RegisterEndpoint(string endPointName, IEndPoint endPoint);
        IConfigure RouteMessage<T>(string endPointName, bool discardIfEndpointDown);

        IConfigure RegisterMessageHandler<T>(IMessageHandler handler);

    }




    internal interface IConfigureInternal {
        void RouteMessage(string typeName, string endPointName, bool discardIfDown);
    }



    internal class Configure : IConfigure, IConfigureInternal {



        private string applicationName = string.Empty;
        private string appGUID = string.Empty;
        private int maxThreads = 1;

        internal EndPoint myEndPoint { get; set; }
        internal IPeristProvider storageProvider { get; set; }
        internal IPersist Persitor { get; set; }

        internal ConcurrentDictionary<string, IEndPoint> EndPoints { get; set; }

        
        internal List<MessageTypeMapping> Routes { get; set; }

        internal int MaxThreads { get { return this.maxThreads; } }

        internal Configure() {
            this.EndPoints = new ConcurrentDictionary<string, IEndPoint>();
            this.Routes = new List<MessageTypeMapping>();

        }

        #region Global



        //IConfigure IConfigure.DisableStartupTests() {
        //    this.testOnStartup = false;
        //    return (IConfigure)this;
        //}
        IConfigure IConfigure.SetApplicationName(string appName) {
            if (string.IsNullOrEmpty(appName)) {
                throw new ArgumentException("appName not provided");
            }
            this.applicationName = appName.Replace(" ", "");
      

            return (IConfigure)this;
        }

        IConfigure IConfigure.MaxThreads(int threadCount) {
            maxThreads = threadCount;
            return (IConfigure)this;
        }

        IConfigure IConfigure.MyEndPoint(EndPoint myEndPoint) {
            this.myEndPoint = myEndPoint;
            return (IConfigure)this;
        }

        IConfigure IConfigure.SetApplicationGUID(string GUID) {
            if (string.IsNullOrEmpty(GUID)) {
                throw new ArgumentException("app GUID not provided");
            }
            this.appGUID = GUID;


            return (IConfigure)this;
        }
        public string ApplicationName {
            get {

                if (string.IsNullOrEmpty(this.applicationName)) {
                    this.applicationName = Util.Util.GetApplicationName().Replace(" ", "").Replace(".", "");
                }

                return this.applicationName;
            }
        }

        public string ApplicationGUID {
            get {
                if (string.IsNullOrEmpty(this.appGUID)) {
                    this.appGUID = Util.Util.GetApplicationGuid();
                }
                return this.appGUID;
            }
        }

        IConfigure IConfigure.RouteMessage<T>(string endPointName, bool discardIfEndpointDown) {


            string typeName = ShortBus.Util.Util.GetTypeName(typeof(T)).ToLower();

            if (!this.Routes.Any(c => c.TypeName.Equals(typeName, StringComparison.OrdinalIgnoreCase) && c.EndPointName.Equals(endPointName, StringComparison.OrdinalIgnoreCase))) {
                this.Routes.Add(new MessageTypeMapping() { TypeName = typeName, EndPointName = endPointName, DiscardIfDown = discardIfEndpointDown });
            }

            return (IConfigure)this;
        }
        void IConfigureInternal.RouteMessage(string typeName, string endPointName, bool discardIfDown) {

            this.Routes.Add(new MessageTypeMapping() { EndPointName = endPointName, TypeName = typeName.ToLower(), DiscardIfDown = discardIfDown });

        }



        IConfigure IConfigure.RegisterMessageHandler<T>(IMessageHandler handler) {
            //handlers
            string messageTypeName = Util.Util.GetTypeName(typeof(T)).ToLower();
            if (this.EndPoints == null) { this.EndPoints = new ConcurrentDictionary<string, IEndPoint>(); }
            this.EndPoints.TryAdd(messageTypeName, handler);

    

            return (IConfigure)this;
        }

        IConfigure IConfigure.PersistTo(IPeristProvider provider) {

            this.storageProvider = provider;

            return (IConfigure)this;
        }

        IConfigure IConfigure.RegisterEndpoint(string endPointName, IEndPoint endPoint) {

            this.EndPoints.TryAdd(endPointName, endPoint);

            return (IConfigure)this;
        }

        #endregion //global


        internal void TestConfig() {
            //if iamasource
            //must have a way to persist
            //must have a publisher configured
            //publisher for each message must exit in publishers.
            //each publisher type must be invokable
            //should have messages
         
            if (this.Persitor == null) {
                this.Persitor = this.storageProvider.CreatePersist(this);
            }

            bool hasEndpoints = ((this.EndPoints != null) && this.EndPoints.Count() > 0);
   

            if (!hasEndpoints) { 
                throw new BusNotConfiguredException("Endpoint", "No Message Endpoints have been added");
            }
           
            //if ((this.Routes != null) && this.Routes.Count() > 0) {

            //    this.Routes.ForEach(m => {

            //    });

            //}


            //if ((this.Messages != null) && (this.Messages.Count() > 0)) {

            //    this.Messages.ForEach(m => {
            //        var match = this.Publishers.FirstOrDefault(p => p.Key.Equals(m.EndPointName, StringComparison.OrdinalIgnoreCase));
            //        if (match.Value == null) {
            //            throw new BusNotConfiguredException("Source", string.Format("Message Type {0} is configured to publish to {1}, but publisher{1} has not been configured", m.TypeName, m.EndPointName));
            //        }

            //    });

            //}

            if (string.IsNullOrEmpty(this.ApplicationName)) {
                throw new BusNotConfiguredException("EndPoint", "Application Name is not configured");
            }
            



            
        }


    }
}
