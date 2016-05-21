

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
        ISubsciberConfigure AsASubscriber { get; }

        string ApplicationName { get; }

        string ApplicationGUID { get; }
        IConfigure DisableStartupTests();
        IConfigure SetApplicationName(string appName);

        bool IsASource { get; }
        bool IsAPublisher { get; }
        bool IsASubscriber { get; }
    }

    public interface ISourceConfigure {
        ISourceConfigure Default(DefaultSourceSettings settings);
        ISourceConfigure RegisterMessage<T>(string publisherName);
        ISourceConfigure RegisterPublisher(IEndPoint publisher, string publisherName);
        ISourceConfigure RegisterDefaultPublisher(IEndPoint publisher);
        ISourceConfigure MaxThreads(int threadCount);
        
    }


    public interface IPublisherConfigure {
        
        IPublisherConfigure Default(DefaultPublisherSettings settings);

        IPublisherConfigure RegisterSubscriber(string subscriberName, IEndPoint subscriber);

        IPublisherConfigure MaxThreads(int threadCount);
        IPublisherConfigure RegisterMessage<T>(string subscriberName);


    }
    internal interface IPublisherConfigureInternal {
        void RegisterMessage(string typeName, string subscriberName);
    }


    public interface ISubsciberConfigure {
        ISubsciberConfigure Default(DefaultSubscriberSettings settings);

        /// <summary>
        /// Map incoming message type to the handler that processes the message
        /// </summary>
        /// <param name="handler"></param>
        /// <param name="messageTypeName"></param>
        /// <returns></returns>
        ISubsciberConfigure RegisterMessageHandler<T>(IMessageHandler handler);

        /// <summary>
        /// Register the publishers from which I expect to recieve messages.
        /// </summary>
        /// <param name="publisherName"></param>
        /// <param name="publisher"></param>
        /// <returns></returns>
        ISubsciberConfigure RegisterPublisher(string publisherName, IEndPoint publisher);

        /// <summary>
        /// Tell the publisher to send messages my way.
        /// </summary>
        /// </remarks>
        /// <param name="messageTypeName"></param>
        /// <param name="publisherName"></param>
        /// <returns></returns>
        ISubsciberConfigure RegisterSubscription<T>(string publisherName);

        ISubsciberConfigure MaxThreads(int threadCount);
    }

    internal class Configure : IConfigure, ISourceConfigure, IPublisherConfigure, ISubsciberConfigure, IPublisherConfigureInternal {

        internal Configure() {

        }

        #region Global

        private bool testOnStartup = true;
        private string applicationName = string.Empty;
        private bool appNameManual = false;

        private bool IAmASource = false;
        private bool IAmAPublisher = false;
        private bool IAmASubscriber = false;

        public bool TestOnStartup {  get { return this.testOnStartup; } }

        public bool IsASource {  get { return this.IAmASource; } }
        public bool IsAPublisher { get { return this.IAmAPublisher; } }
        public bool IsASubscriber { get { return this.IAmASubscriber; } }

        internal Dictionary<string, IEndPoint> Publishers { get; set; }

        internal List<MessageTypeMapping> Subscriptions { get; set; }

        IConfigure IConfigure.DisableStartupTests() {
            this.testOnStartup = false;
            return (IConfigure)this;
        }
        IConfigure IConfigure.SetApplicationName(string appName) {
            if (string.IsNullOrEmpty(appName)) {
                throw new ArgumentException("appName not provided");
            }
            this.applicationName = appName.Replace(" ", "");
            this.appNameManual = true;

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

        string IConfigure.ApplicationGUID {
            get {
                return Util.Util.GetApplicationGuid();
            }
        }

        #endregion //global

        #region Source

        internal int maxSourceThreads = 1;
        internal IPersist source_LocalStorage { get; set; }
        internal IPersist source_ConfigStorage { get; set; }
        internal List<MessageTypeMapping> Messages { get; set; }

        internal void TestSourceConfig() {
            //if iamasource
            //must have a way to persist
            //must have a publisher configured
            //publisher for each message must exit in publishers.
            //each publisher type must be invokable
            //should have messages
            if (this.IAmASource) {
                if (this.source_LocalStorage == null) {
                    throw new BusNotConfiguredException("Source", "Local Storage has not been configured");
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
        ISourceConfigure ISourceConfigure.Default(DefaultSourceSettings settings) {

            //setup local storage
            MongoPersistSettings dbSettings = new MongoPersistSettings() {
                ConnectionString = settings.MongoConnectionString
                , Collection = "source"
                , DB = this.ApplicationName
            };


            this.source_LocalStorage = new MongoPersist(dbSettings);
            //setup publisher
            this.source_ConfigStorage = new MongoPersist(new MongoPersistSettings() {
                Collection = "source_config"
                , ConnectionString = settings.MongoConnectionString
                , DB = this.ApplicationName
            });


            ((ISourceConfigure)this).RegisterDefaultPublisher(new RESTEndPoint(settings.PublisherSettings));


            return (ISourceConfigure)this;

        }

        ISourceConfigure ISourceConfigure.RegisterMessage<T>(string publisherName) {
            if (this.Messages == null) this.Messages = new List<MessageTypeMapping>();

            string typeName = ShortBus.Util.Util.GetTypeName(typeof(T)).ToLower();

            if (!this.Messages.Any(c => c.TypeName.Equals(typeName, StringComparison.OrdinalIgnoreCase) && c.EndPointName.Equals(publisherName, StringComparison.OrdinalIgnoreCase))) {
                this.Messages.Add(new MessageTypeMapping() { TypeName = typeName, EndPointName = publisherName });
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
        ISourceConfigure ISourceConfigure.RegisterDefaultPublisher(IEndPoint publisher) {
            return ((ISourceConfigure)this).RegisterPublisher(publisher, "Default");
        }

        ISourceConfigure ISourceConfigure.MaxThreads(int threadCount) {
            maxSourceThreads = threadCount;
            return (ISourceConfigure)this;
        }

        #endregion

        #region Publisher

        internal int maxPublisherThreads = 1;
        
        internal IPersist publisher_LocalStorage { get; set; }
        internal IPersist publisher_ConfigStorage { get; set; }
        internal ConcurrentDictionary<string, IEndPoint> Subscribers { get; set; }


        internal void TestPublisherConfig() {
            if (IAmAPublisher) {

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
        IPublisherConfigure IPublisherConfigure.Default(DefaultPublisherSettings settings) {
            //setup local storage
            MongoPersistSettings dbSettings = new MongoPersistSettings() {
                ConnectionString = settings.MongoConnectionString
                , Collection = "publish"
                , DB = this.ApplicationName
            };

            this.publisher_LocalStorage = new MongoPersist(dbSettings);
            this.publisher_ConfigStorage = new MongoPersist(new MongoPersistSettings() {
                Collection = "publisher_config"
                , ConnectionString = settings.MongoConnectionString
                , DB = this.ApplicationName
            });

            return (IPublisherConfigure)this;

        }

        IPublisherConfigure IPublisherConfigure.RegisterSubscriber(string subscriberName, IEndPoint subscriber) {

            this.Subscribers.TryAdd(subscriberName, subscriber);

            return (IPublisherConfigure)this;
        }

        IPublisherConfigure IPublisherConfigure.MaxThreads(int threadCount) {
            this.maxPublisherThreads = threadCount;
            return ((IPublisherConfigure)this);
        }

        void IPublisherConfigureInternal.RegisterMessage(string typeName, string subscriberName) {
            this.Subscriptions.Add(new MessageTypeMapping() { EndPointName = subscriberName, TypeName = typeName.ToLower() });
       
        }

        IPublisherConfigure IPublisherConfigure.RegisterMessage<T>(string subscriberName) {

            string typeName = Util.Util.GetTypeName(typeof(T)).ToLower();
            this.Subscriptions.Add(new MessageTypeMapping() { EndPointName = subscriberName, TypeName = typeName });
            return (IPublisherConfigure)this;
        }

        #endregion

        #region Subscriber

        internal IPersist subscriber_LocalStorage { get; set; }
        internal IPersist subscriber_ConfigStorage { get; set; }
        internal void TestSubscriberConfig() {
            if (IAmASubscriber) {


                if (string.IsNullOrEmpty(this.ApplicationName)) {
                    throw new BusNotConfiguredException("Subscriber", "Application Name is not configured");
                }
            }
        }
        internal string endPointAddress { get; set; }
        ISubsciberConfigure IConfigure.AsASubscriber {
            get {
                this.IAmASubscriber = true;
                return (ISubsciberConfigure)this;
            }
        }

        internal int MaxSubscriberThreads = 1;

        internal Dictionary<string, IMessageHandler> Handlers;
        

        ISubsciberConfigure ISubsciberConfigure.MaxThreads(int threads) {
            this.MaxSubscriberThreads = threads;
            return (ISubsciberConfigure)this;
        }

        ISubsciberConfigure ISubsciberConfigure.Default(DefaultSubscriberSettings settings) {
            MongoPersistSettings dbSettings = new MongoPersistSettings() {
                ConnectionString = settings.MongoConnectionString
                , Collection = "subscribe"
                , DB = this.ApplicationName
            };

            this.subscriber_LocalStorage = new MongoPersist(dbSettings);
            this.subscriber_ConfigStorage = new MongoPersist(new MongoPersistSettings() {
                Collection = "subscriber_config"
                , ConnectionString = settings.MongoConnectionString
                , DB = this.ApplicationName
            });
            this.endPointAddress = settings.Endpoint.URL;

            if (Publishers == null) { Publishers = new Dictionary<string, IEndPoint>(); }
            Publishers.Add("Default", new RESTEndPoint(settings.Publisher));
            

            return (ISubsciberConfigure)this;
        }

        public ISubsciberConfigure RegisterMessageHandler<T>(IMessageHandler handler) {
            //handlers
            string messageTypeName = Util.Util.GetTypeName(typeof(T)).ToLower();
            if (this.Handlers == null) { this.Handlers = new Dictionary<string, IMessageHandler>(); }
            this.Handlers.Add(messageTypeName, handler);
            return (ISubsciberConfigure)this;
        }

        public ISubsciberConfigure RegisterPublisher(string publisherName, IEndPoint publisher) {
            //publishers
            if (Publishers == null) { Publishers = new Dictionary<string, IEndPoint>(); }
            Publishers.Add(publisherName, publisher);
            return (ISubsciberConfigure)this;
        }

        public ISubsciberConfigure RegisterSubscription<T>( string publisherName) {
            //subscriptions
            string messageTypeName = Util.Util.GetTypeName(typeof(T)).ToLower();
            if (Subscriptions == null) Subscriptions = new List<MessageTypeMapping>();
            Subscriptions.Add(new MessageTypeMapping() { EndPointName = publisherName, TypeName = messageTypeName });
            return (ISubsciberConfigure)this;
        }




        #endregion





    }
}
