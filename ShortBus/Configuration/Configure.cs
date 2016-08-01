

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

        /// <summary>
        /// Returns Configure's ISourceConfig Implementation
        /// </summary>
        /// <remarks>
        /// <list type="Bullet">
        /// <item>
        /// If Invoked, automatically configures Bus as a Source, and 
        /// therefore Bus will require that is properly configured as a Source when started.
        /// </item>
        /// </list>
        /// </remarks>
        ISourceConfigure AsASource { get; }
        IPublisherConfigure AsAPublisher { get; }
        ISubscriberConfigure AsASubscriber { get; }

        string ApplicationName { get; }

        string ApplicationGUID { get; }

        IConfigure SetApplicationName(string appName);
        IConfigure SetApplicationGUID(string appGUID);

        IConfigure PersistTo(IPeristProvider provider);

        IConfigure MyEndPoint(EndPoint myEndPoint);

        bool IsASource { get; }
        bool IsAPublisher { get; }
        bool IsASubscriber { get; }
    }

    public interface ISourceConfigure {
        
       
        ISourceConfigure RegisterMessage<T>(string publisherName);
        ISourceConfigure RegisterPublisher(IEndPoint publisher, string publisherName);
        ISourceConfigure MaxThreads(int threadCount);
        
    }


    public interface IPublisherConfigure {
        
        //IPublisherConfigure Default(DefaultPublisherSettings settings);

        IPublisherConfigure RegisterSubscriber(string subscriberName, IEndPoint subscriber);

        IPublisherConfigure MaxThreads(int threadCount);
        IPublisherConfigure RegisterMessage<T>(string subscriberName, bool discardIfSubscriberIsDown);


    }
    internal interface IPublisherConfigureInternal {
        void RegisterMessage(string typeName, string subscriberName, bool discardIfDown);
    }


    public interface ISubscriberConfigure {
        //ISubscriberConfigure Default(DefaultSubscriberSettings settings);

        /// <summary>
        /// Map incoming message type to the handler that processes the message
        /// </summary>
        /// <param name="handler"></param>
        /// <param name="messageTypeName"></param>
        /// <returns></returns>
        ISubscriberConfigure RegisterMessageHandler<T>(IMessageHandler handler);

        /// <summary>
        /// Register the publishers from which I expect to recieve messages.
        /// </summary>
        /// <param name="publisherName"></param>
        /// <param name="publisher"></param>
        /// <returns></returns>
        ISubscriberConfigure RegisterPublisher(string publisherName, IEndPoint publisher);

        /// <summary>
        /// Tell the publisher to send messages my way.
        /// </summary>
        /// </remarks>
        /// <param name="messageTypeName"></param>
        /// <param name="publisherName"></param>
        /// <returns></returns>
        ISubscriberConfigure RegisterSubscription<T>(string publisherName, bool discardIfSubscriberIsDown);

        ISubscriberConfigure MaxThreads(int threadCount);
    }

    internal class Configure : IConfigure, ISourceConfigure, IPublisherConfigure, ISubscriberConfigure, IPublisherConfigureInternal {

        internal Configure() {

        }

        #region Global

      
        private string applicationName = string.Empty;
        private string appGUID = string.Empty;

        private bool IAmASource = false;
        private bool IAmAPublisher = false;
        private bool IAmASubscriber = false;



        public bool IsASource {  get { return this.IAmASource; } }
        public bool IsAPublisher { get { return this.IAmAPublisher; } }
        public bool IsASubscriber { get { return this.IAmASubscriber; } }

        internal Dictionary<string, IEndPoint> Publishers { get; set; }

        internal List<MessageTypeMapping> Subscriptions { get; set; }

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

        #endregion //global

        #region Source

        internal int maxSourceThreads = 1;
        internal IPersist source_LocalStorage { get; set; }
        internal List<MessageTypeMapping> Messages { get; set; }
        internal IPeristProvider storageProvider { get; set; }
        internal void TestSourceConfig() {
            //if iamasource
            //must have a way to persist
            //must have a publisher configured
            //publisher for each message must exit in publishers.
            //each publisher type must be invokable
            //should have messages
            if (this.IAmASource) {


                if (this.source_LocalStorage == null) {
                    this.source_LocalStorage = this.storageProvider.CreatePersist(this, EndPointTypeOptions.Source);
                    //throw new BusNotConfiguredException("Source", "Local Storage has not been configured");
                }
                if (this.Publishers == null || this.Publishers.Count() == 0) {
                    throw new BusNotConfiguredException("Source", "No publishers have been configured");
                }
                if ((this.Messages != null) && (this.Messages.Count() > 0)) {

                    this.Messages.ForEach(m => {
                        var match = this.Publishers.FirstOrDefault(p => p.Key.Equals(m.EndPointName, StringComparison.OrdinalIgnoreCase));
                        if (match.Value == null) {
                            throw new BusNotConfiguredException("Source", string.Format("Message Type {0} is configured to publish to {1}, but publisher{1} has not been configured", m.TypeName, m.EndPointName));
                        }

                    });

                }
                if (string.IsNullOrEmpty(this.ApplicationName)) {
                    throw new BusNotConfiguredException("Source", "Application Name is not configured");
                }
            }
        }

        ISourceConfigure IConfigure.AsASource {
            get {
                this.IAmASource = true;
                this.Publishers = new Dictionary<string, IEndPoint>();
                this.Messages = new List<MessageTypeMapping>();
                return (ISourceConfigure)this;
            }
        }

        IConfigure IConfigure.PersistTo(IPeristProvider provider) {

            this.storageProvider = provider;

            return (IConfigure)this;
        }


        ISourceConfigure ISourceConfigure.RegisterMessage<T>(string publisherName) {
            if (this.Messages == null) this.Messages = new List<MessageTypeMapping>();

            string typeName = ShortBus.Util.Util.GetTypeName(typeof(T)).ToLower();

            if (!this.Messages.Any(c => c.TypeName.Equals(typeName, StringComparison.OrdinalIgnoreCase) && c.EndPointName.Equals(publisherName, StringComparison.OrdinalIgnoreCase))) {
                this.Messages.Add(new MessageTypeMapping() { TypeName = typeName, EndPointName = publisherName, DiscardIfDown = false });
            }

            return (ISourceConfigure)this;
        }

        ISourceConfigure ISourceConfigure.RegisterPublisher(IEndPoint publisher, string publisherName) {
            if (this.Publishers == null) this.Publishers = new Dictionary<string, IEndPoint>();
            if (this.Publishers.Any(g => g.Key.Equals(publisherName, StringComparison.OrdinalIgnoreCase))) {
                this.Publishers.Remove(publisherName);
            }
            this.Publishers.Add(publisherName, publisher);
            return (ISourceConfigure)this;
        }

        ISourceConfigure ISourceConfigure.MaxThreads(int threadCount) {
            maxSourceThreads = threadCount;
            return (ISourceConfigure)this;
        }

        #endregion

        #region Publisher

        internal int maxPublisherThreads = 1;
        
        internal IPersist publisher_LocalStorage { get; set; }
        internal ConcurrentDictionary<string, IEndPoint> Subscribers { get; set; }


        internal void TestPublisherConfig() {
            if (IAmAPublisher) {

                if (this.publisher_LocalStorage == null) {
                    this.publisher_LocalStorage = this.storageProvider.CreatePersist(this, EndPointTypeOptions.Publisher);
                    //throw new BusNotConfiguredException("Source", "Local Storage has not been configured");
                }

                if (string.IsNullOrEmpty(this.ApplicationName)) {
                    throw new BusNotConfiguredException("Publisher", "Application Name is not configured");
                }
            }
        }

        IPublisherConfigure IConfigure.AsAPublisher {
            get {
                this.IAmAPublisher = true;
                this.Subscribers = new ConcurrentDictionary<string, IEndPoint>();
                this.Subscriptions = new List<MessageTypeMapping>();
                return (IPublisherConfigure)this;
            }
        }
        //IPublisherConfigure IPublisherConfigure.Default(DefaultPublisherSettings settings) {
        //    //setup local storage
        //    MongoPersistSettings dbSettings = new MongoPersistSettings() {
        //        ConnectionString = settings.MongoConnectionString
        //        , Collection = "publish"
        //        , DB = this.ApplicationName
        //    };

        //    this.publisher_LocalStorage = new MongoPersist(dbSettings);
        //    this.publisher_ConfigStorage = new MongoPersist(new MongoPersistSettings() {
        //        Collection = "publisher_config"
        //        , ConnectionString = settings.MongoConnectionString
        //        , DB = this.ApplicationName
        //    });

        //    return (IPublisherConfigure)this;

        //}

        IPublisherConfigure IPublisherConfigure.RegisterSubscriber(string subscriberName, IEndPoint subscriber) {

            this.Subscribers.TryAdd(subscriberName, subscriber);

            return (IPublisherConfigure)this;
        }

        IPublisherConfigure IPublisherConfigure.MaxThreads(int threadCount) {
            this.maxPublisherThreads = threadCount;
            return ((IPublisherConfigure)this);
        }

        void IPublisherConfigureInternal.RegisterMessage(string typeName, string subscriberName, bool discardIfDown) {
            this.Subscriptions.Add(new MessageTypeMapping() { EndPointName = subscriberName, TypeName = typeName.ToLower(), DiscardIfDown = discardIfDown });
       
        }

        IPublisherConfigure IPublisherConfigure.RegisterMessage<T>(string subscriberName, bool discardIfSubscriberIsDown) {

            string typeName = Util.Util.GetTypeName(typeof(T)).ToLower();
            this.Subscriptions.Add(new MessageTypeMapping() { EndPointName = subscriberName, TypeName = typeName, DiscardIfDown = discardIfSubscriberIsDown });
            return (IPublisherConfigure)this;
        }

        #endregion

        #region Subscriber

        internal IPersist subscriber_LocalStorage { get; set; }

        internal void TestSubscriberConfig() {
            if (IAmASubscriber) {

                if (this.subscriber_LocalStorage == null) {
                    this.subscriber_LocalStorage = this.storageProvider.CreatePersist(this, EndPointTypeOptions.Subscriber);
                    //throw new BusNotConfiguredException("Source", "Local Storage has not been configured");
                }

                if (string.IsNullOrEmpty(this.ApplicationName)) {
                    throw new BusNotConfiguredException("Subscriber", "Application Name is not configured");
                }
            }
        }
        internal EndPoint myEndPoint { get; set; }
        ISubscriberConfigure IConfigure.AsASubscriber {
            get {
                this.IAmASubscriber = true;
                return (ISubscriberConfigure)this;
            }
        }

        internal int MaxSubscriberThreads = 1;

        internal Dictionary<string, IMessageHandler> Handlers;
        

        ISubscriberConfigure ISubscriberConfigure.MaxThreads(int threads) {
            this.MaxSubscriberThreads = threads;
            return (ISubscriberConfigure)this;
        }

        //ISubscriberConfigure ISubscriberConfigure.Default(DefaultSubscriberSettings settings) {
        //    MongoPersistSettings dbSettings = new MongoPersistSettings() {
        //        ConnectionString = settings.MongoConnectionString
        //        , Collection = "subscribe"
        //        , DB = this.ApplicationName
        //    };

        //    this.subscriber_LocalStorage = new MongoPersist(dbSettings);
        //    this.subscriber_ConfigStorage = new MongoPersist(new MongoPersistSettings() {
        //        Collection = "subscriber_config"
        //        , ConnectionString = settings.MongoConnectionString
        //        , DB = this.ApplicationName
        //    });
        //    this.endPointAddress = settings.Endpoint.URL;

        //    if (Publishers == null) { Publishers = new Dictionary<string, IEndPoint>(); }
        //    Publishers.Add("Default", new RESTEndPoint(settings.Publisher));
            

        //    return (ISubscriberConfigure)this;
        //}

        public ISubscriberConfigure RegisterMessageHandler<T>(IMessageHandler handler) {
            //handlers
            string messageTypeName = Util.Util.GetTypeName(typeof(T)).ToLower();
            if (this.Handlers == null) { this.Handlers = new Dictionary<string, IMessageHandler>(); }
            this.Handlers.Add(messageTypeName, handler);
            return (ISubscriberConfigure)this;
        }

        public ISubscriberConfigure RegisterPublisher(string publisherName, IEndPoint publisher) {
            //publishers
            if (Publishers == null) { Publishers = new Dictionary<string, IEndPoint>(); }
            Publishers.Add(publisherName, publisher);
            return (ISubscriberConfigure)this;
        }

        public ISubscriberConfigure RegisterSubscription<T>( string publisherName, bool discardIfSubscriberDown) {
            //subscriptions
            string messageTypeName = Util.Util.GetTypeName(typeof(T)).ToLower();
            if (Subscriptions == null) Subscriptions = new List<MessageTypeMapping>();
            Subscriptions.Add(new MessageTypeMapping() { EndPointName = publisherName, TypeName = messageTypeName, DiscardIfDown = discardIfSubscriberDown });
            return (ISubscriberConfigure)this;
        }




        #endregion





    }
}
