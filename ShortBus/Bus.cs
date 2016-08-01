using ShortBus.Configuration;
using ShortBus.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using ShortBus.Publish;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using ShortBus.Default;
using ShortBus.Subscriber;
using System.Transactions;

namespace ShortBus {

    class TxRM : IEnlistmentNotification {

        public TxRM() { }

        private IEnumerable<PersistedMessage> messages = null;
        private IPersist persistor = null;
        private string txId = string.Empty;
        private Task whenDone = null;

        public string SendMessage(IEnumerable<PersistedMessage> messages, IPersist persistor, Task whenDone) {

            string txId = string.Empty;
            Transaction currentTx = Transaction.Current;
            if (currentTx != null) {

                txId = currentTx.TransactionInformation.LocalIdentifier;
                this.persistor = persistor;
                this.messages = messages;
                currentTx.EnlistVolatile(this, EnlistmentOptions.None);
                this.whenDone = whenDone;

            } else {
                txId = Guid.NewGuid().ToString();
                foreach(var m in messages) {
                    m.TransactionID = txId;
                }
                persistor.Persist(messages);
                whenDone.Start();
            }

            return txId;

        }

        void IEnlistmentNotification.Commit(Enlistment enlistment) {
            persistor.CommitBatch(this.txId);
            enlistment.Done();
            whenDone.Start();
        }

        void IEnlistmentNotification.InDoubt(Enlistment enlistment) {
            enlistment.Done();
        }

        void IEnlistmentNotification.Prepare(PreparingEnlistment preparingEnlistment) {

            foreach (PersistedMessage m in messages) {
                m.Status = PersistedMessageStatusOptions.Uncommitted;
                m.TransactionID = this.txId;
            }
            persistor.Persist(messages);
            
            preparingEnlistment.Prepared();

        }

        void IEnlistmentNotification.Rollback(Enlistment enlistment) {
            //don't need to do anything, the messages are not marked for processing
            //errr. i guess we could delete them
            enlistment.Done();
        }
    }

    public class Bus {

      
        //#TODO - Define Event Args for Bus Events, possibly create more specic events
        public delegate void BusStarted(object sender, EventArgs args);
        public static event BusStarted OnStarted;
        public delegate void BusProcessing(object sender, EventArgs args);
        public static event BusProcessing OnProcessing;
        public delegate void ThreadStalled(object sender, EventArgs args);
        public static event ThreadStalled OnStalled;
        public delegate void ThreadStarted(object sender, EventArgs args);
        public static event ThreadStarted OnThreadStarted;

        //Lets the subscriber know to restart processing after a new subscription is created
        //Timer the handler for new subscriptions will start the timer to restart processing
        private static volatile bool newSubscribers = false;
        private static System.Timers.Timer publisherRestartTimer = new System.Timers.Timer();
        private static System.Timers.Timer subscriberRestartTimer = null;
        

        static Bus() {

            configure = new Configure();

        }

        //Fluent configuration
        private static Configure configure;
        public static IConfigure Configure { get { return (IConfigure)configure; } }


        //Each node must have a unique name... defaults to assembly name, but
        //can be overridden
        public static string ApplicationName {
            get {
                return ((IConfigure)configure).ApplicationName;
            }
        }

        //synchronization mechanism
        struct pubStat {
            public bool Has { get; set; }
            public bool Processing { get; set; }
        }


        /// <summary>
        /// Processes all subscription requests.  Subscription requests are sent to the
        /// publisher as normal messages, and processed as such.
        /// A subscription request will stop all processing to allow the new subscription
        /// to be registered, then the Bus will restart processing.
        /// </summary>
        class SubscriptionSubscriber : IEndPoint {
            EndpointResponse IEndPoint.HelloWorld(PersistedMessage message) {
                return new EndpointResponse() { Status = true, PayLoad = null };
            }

            EndpointResponse IEndPoint.Publish(PersistedMessage message) {

                /* subscriber is requesting that I (as a publisher) send messages its way.
                 * I have to make sure the subscription is persisted, and added to my in memory subscribers
                */
                string payLoad = message.PayLoad;
                SubscriptionRequest request = Newtonsoft.Json.JsonConvert.DeserializeObject<SubscriptionRequest>(payLoad);

                EndpointConfigPersist<EndPointConfig> config = new EndpointConfigPersist<EndPointConfig>(configure.publisher_LocalStorage);
                EndPointConfig stored = config.GetConfig();

                bool changed = false;
                if (request.DeRegister) {

                    bool subscriberStored = false, subscriptionStored = false, lastSubscription = false;
                    subscriberStored = stored.Subscribers().Any(g => g.Name.Equals(request.Name, StringComparison.OrdinalIgnoreCase));
                    subscriptionStored = stored.Subscriptions.Any(g => g.TypeName.Equals(request.MessageTypeName, StringComparison.Ordinal)
                        && g.EndPointName.Equals(request.Name, StringComparison.OrdinalIgnoreCase));
                    lastSubscription = stored.Subscriptions.Count(g => g.EndPointName.Equals(request.Name, StringComparison.OrdinalIgnoreCase)) == 1;
                    if (subscriberStored && lastSubscription) {
                        stored.RemoveEndPoint(request.Name);
                        changed = true;
                    }
                    if (subscriptionStored) {
                        MessageTypeMapping m = stored.Subscriptions.FirstOrDefault(g => g.TypeName.Equals(request.MessageTypeName, StringComparison.Ordinal)
                        && g.EndPointName.Equals(request.Name, StringComparison.OrdinalIgnoreCase));
                        stored.Subscriptions.Remove(m);
                        changed = true;
                    }
                    if (changed) {
                        config.UpdateConfig(stored);
                    }
                    /* if removing subscription
                        - remove subscriber if no other subscriptions
                        - remove subscription if there
                        - remove from subscribers running if no other subscriptions (set currentsubscriber = 0)
                        - remove from configure.subscriptions if there
                    */



                    MessageTypeMapping c = configure.Subscriptions.FirstOrDefault(g => g.EndPointName.Equals(request.Name, StringComparison.OrdinalIgnoreCase) && g.TypeName.Equals(request.MessageTypeName, StringComparison.OrdinalIgnoreCase));
                    if (!string.IsNullOrEmpty(c.EndPointName)) {
                        configure.Subscriptions.Remove(c);
                    }
                    //if no other subscriptions, remove subscriber
                    if (!configure.Subscriptions.Any(g => g.EndPointName.Equals(request.Name, StringComparison.OrdinalIgnoreCase))) {
                        IEndPoint toRemove = null;
                        pubStat statToRemove;
                        subscribersRunning.TryRemove(request.Name, out statToRemove);
                        currentSubscriber = 0;
                        configure.Subscribers.TryRemove(request.Name, out toRemove);
                        
                    }

                    newSubscribers = true;



                } else {
                    /*
                     * add subscriber if not there
                     * add subscription if not there
                     * add to subscribers running if not there
                     * add to configure.subscriptions if not there
                     */
                    bool subscriberStored = false, subscriptionStored = false;
                    subscriberStored = stored.Subscribers().Any(g => g.Name.Equals(request.Name, StringComparison.OrdinalIgnoreCase));
                    subscriptionStored = stored.Subscriptions.Any(g => g.TypeName.Equals(request.MessageTypeName, StringComparison.Ordinal)
                        && g.EndPointName.Equals(request.Name, StringComparison.OrdinalIgnoreCase));
                    
                    if (!subscriberStored) {
                        stored.AddEndPoint(new EndPoint() {
                            Name = request.Name
                                , EndPointAddress = request.EndPoint
                                , EndPointType = EndPointTypeOptions.Subscriber
                        });
                        
                        changed = true;
                    } else {
                        //if subscriber stored... make sure the endpoint is the same...
                        EndPoint match = stored.Subscribers().FirstOrDefault(g => g.Name.Equals(request.Name, StringComparison.OrdinalIgnoreCase));
                        if (!match.EndPointAddress.Equals(request.EndPoint, StringComparison.OrdinalIgnoreCase)) {
                            stored.RemoveEndPoint(request.Name);
                            stored.AddEndPoint(new EndPoint() {
                                Name = request.Name
                                , EndPointAddress = request.EndPoint
                                , EndPointType = EndPointTypeOptions.Subscriber
                            });
                            
                            changed = true;
                        }
                    }
                    if (!subscriptionStored) {
                        
                        stored.Subscriptions.Add(new MessageTypeMapping() { EndPointName = request.Name, TypeName = request.MessageTypeName, DiscardIfDown = request.DiscardIfSubscriberIsDown });
                        changed = true;
                    }
                    if (changed) {
                        config.UpdateConfig(stored);
                    }


                    //if subscriber not registered, add it
                    IEndPoint registeredSubscriber = configure.Subscribers.FirstOrDefault(g => g.Key.Equals(request.Name, StringComparison.OrdinalIgnoreCase)).Value;
                    if (registeredSubscriber == null) {
                        IEndPoint toAdd = new RESTEndPoint(new RESTSettings() { URL = request.EndPoint });

                        ((IPublisherConfigure)configure).RegisterSubscriber(request.Name, toAdd);

                        subscribersRunning.AddOrUpdate(request.Name, new pubStat() { Has = true, Processing = false }, (r, z) => { return new pubStat() { Has = true, Processing = false }; });

                    } 

                    if (!configure.Subscribers.Any(g => g.Key.Equals(request.Name, StringComparison.OrdinalIgnoreCase))) {
                        IEndPoint toAdd = new RESTEndPoint(new RESTSettings() { URL = request.EndPoint });

                        ((IPublisherConfigure)configure).RegisterSubscriber(request.Name, toAdd);

                        subscribersRunning.AddOrUpdate(request.Name, new pubStat() { Has = true, Processing = false }, (r, z) => { return new pubStat() { Has = true, Processing = false }; });
                        
                        
                    }

                    MessageTypeMapping c = configure.Subscriptions.FirstOrDefault(g => g.EndPointName.Equals(request.Name, StringComparison.OrdinalIgnoreCase) && g.TypeName.Equals(request.MessageTypeName, StringComparison.OrdinalIgnoreCase));
                    if (c == null) {
                        ((IPublisherConfigureInternal)configure).RegisterMessage(request.MessageTypeName, request.Name, request.DiscardIfSubscriberIsDown);
                        
                    }

                    //if the endpoint was down, reset it (since we now know its up)
                    IEndPoint subscriber = configure.Subscribers.FirstOrDefault(g => g.Key.Equals(request.Name, StringComparison.OrdinalIgnoreCase)).Value;
                   
                    subscriber.ResetConnection(request.EndPoint);
                    //unmark all messages so bus resends
                    configure.publisher_LocalStorage.UnMarkAll(request.Name);


                    subscribersRunning.AddOrUpdate(request.Name, new pubStat() { Has = true, Processing = false }, (k, v) => {
                        return new pubStat() { Has = true, Processing = false };
                    });


              

                    newSubscribers = true;

                }

                return new EndpointResponse() { Status = true };
   
            }

            //running in process
            bool IEndPoint.ResetConnection() {
                return true;
            }
            bool IEndPoint.ResetConnection(string endPointAddress) {
                return true;
            }

            bool IEndPoint.ServiceIsDown() {
                return false;
            }
        }

        class EndpointRegistrationSubscriber : IEndPoint {
            EndpointResponse IEndPoint.HelloWorld(PersistedMessage message) {
                return new EndpointResponse() { Status = true, PayLoad = null };
            }

            EndpointResponse IEndPoint.Publish(PersistedMessage message) {

                /* source is requesting that I (as a publisher) publish it's messaged
                 * I have to make sure the endpoint is registered so I can call back if needed.
                */
                string payLoad = message.PayLoad;
                EndpointRegistrationRequest request = Newtonsoft.Json.JsonConvert.DeserializeObject<EndpointRegistrationRequest>(payLoad);

                EndpointConfigPersist<EndPointConfig> config = new EndpointConfigPersist<EndPointConfig>(configure.publisher_LocalStorage);
                EndPointConfig stored = config.GetConfig();

                bool changed = false;
                if (request.DeRegister) {

                  

                } else {
                   

                }

                return new EndpointResponse() { Status = true };

            }

            //running in process
            bool IEndPoint.ResetConnection() {
                return true;
            }
            bool IEndPoint.ResetConnection(string endPointAddress) {
                return true;
            }

            bool IEndPoint.ServiceIsDown() {
                return false;
            }
        }

        //all messages are processed by incoming date/time.  Since it may be
        //possible that two messages have same date/time, each messages is given
        //an ordinal when first persisted. Bus will sort by date/time and ordinal to
        //ensure messages are processed in the order they were taken.
        //ordinal is reset to zero if more than 5 seconds have passed.
        private static int ordinal = 0;
        private static DateTime padTime = DateTime.Now;
        private static int SyncGetPadding() {

            lock (sourceLock) {
                ordinal++;
                TimeSpan span = DateTime.Now - padTime;
                if (span.TotalSeconds > 5) {
                    ordinal = 1;
                    padTime = DateTime.Now;
                }

            }
            return ordinal;
        }


        //syncronization mechanism
        private readonly static object sourceLock = new object();
        private static CancellationTokenSource CTS = null;
        private static bool stopped = true;
        private static bool stopping = false;
        private static ManualResetEvent allStopped = new ManualResetEvent(true);

        
        private static void ResolvePersistedConfig() {
            if (configure.IsASource) {
                
                EndPointConfig stored = null;
                EndpointConfigPersist<EndPointConfig> sourceConfig = new EndpointConfigPersist<EndPointConfig>(configure.source_LocalStorage);
                

                stored = sourceConfig.GetConfig();
                if (stored == null) {
                    stored = new EndPointConfig() {
                        ApplicationGUID = ((IConfigure)configure).ApplicationGUID
                        , ApplicationName = ((IConfigure)configure).ApplicationName
                        , Version = "1.0"
                    };
                    sourceConfig.UpdateConfig(stored);
                }

                
                

            }

            if (configure.IsAPublisher) {

                EndPointConfig stored = null;
                EndpointConfigPersist<EndPointConfig> pubConfig = new EndpointConfigPersist<EndPointConfig>(configure.publisher_LocalStorage);


                stored = pubConfig.GetConfig();
                if (stored == null) {
                    stored = new EndPointConfig() {
                        ApplicationGUID = ((IConfigure)configure).ApplicationGUID
                        , ApplicationName = ((IConfigure)configure).ApplicationName
                        , Version = "1.0"
                        , EndPoints = new List<EndPoint>()
                        , Subscriptions = new List<MessageTypeMapping>()
                    };
                    pubConfig.UpdateConfig(stored);
                }

                foreach (EndPoint s in stored.Subscribers()) {
                    ((IPublisherConfigure)configure).RegisterSubscriber(s.Name, new RESTEndPoint(new RESTSettings() { URL = s.EndPointAddress }));

                }
                foreach (MessageTypeMapping map in stored.Subscriptions) {
                    ((IPublisherConfigureInternal)configure).RegisterMessage(map.TypeName, map.EndPointName, map.DiscardIfDown);
                }


            }

            if (configure.IsASubscriber) {

                EndPointConfig stored = null;
                EndpointConfigPersist<EndPointConfig> subConfig = new EndpointConfigPersist<EndPointConfig>(configure.subscriber_LocalStorage);


                stored = subConfig.GetConfig();
                if (stored == null) {
                    stored = new EndPointConfig() {
                        ApplicationGUID = ((IConfigure)configure).ApplicationGUID
                        , ApplicationName = ((IConfigure)configure).ApplicationName
                        , Version = "1.0"
                    };
                    subConfig.UpdateConfig(stored);
                }

            }
        }

        private static bool AllThreadsAreStopped() {

            bool allThreadsAreStopped = true;
            if (pubThreadsRunning != null && pubThreadsRunning.Count() > 0) {
                allThreadsAreStopped = allThreadsAreStopped && (!pubThreadsRunning.Any(g => g.Value));

            }
            if (sourceThreadsRunning != null && sourceThreadsRunning.Count() > 0) {
                allThreadsAreStopped = allThreadsAreStopped && (!sourceThreadsRunning.Any(g => g.Value));
            }
            if (subThreadsRunning != null && subThreadsRunning.Count() > 0) {
                allThreadsAreStopped = allThreadsAreStopped && (!subThreadsRunning.Any(g => g.Value));
            }
            return allThreadsAreStopped;
        }

        private static void Stall(int threadId) {

            
            if (AllThreadsAreStopped()) {
                
                if (stopping) {
                    allStopped.Set();
                } else { //if we are stopping the bus, don't restart the threads... that would be silly!
                    if (newSubscribers && publisherRestartTimer != null) {
                        newSubscribers = false;

                        publisherRestartTimer.Start();
                    }
                    if (subscriberRestartTimer != null) {
                        subscriberRestartTimer.Start();
                    }
                }
            }


            RaiseOnStalled();
        }
        private static void RaiseOnStarted() {
            if (OnStarted != null) { OnStarted(null, EventArgs.Empty); }
        }
        private static void RaiseOnThreadStarted() {
            if (OnThreadStarted != null) { OnThreadStarted(null, EventArgs.Empty); }
        }
        private static void RaiseOnProcessing() {
            if (OnProcessing != null) { OnProcessing(null, EventArgs.Empty); }
        }
        private static void RaiseOnStalled() {
            if (OnStalled != null) { OnStalled(null, EventArgs.Empty); }
        }

        private static void StopGraceful() {
            stopping = true;


            //if all threads are stopped... yipee
            //o/w, reset allstopped and wait for all threads to stop (for 15 seconds)
            if (!AllThreadsAreStopped()) {
                allStopped.Reset();
                allStopped.WaitOne(15000);
                allStopped.Set();
            }

            stopping = false;
            stopped = true;          

        }

        public static void Stop(bool Immediate) {
            //immediately notify any running threads that they should stop.

            if (!Immediate) {
                stopping = true;
                StopGraceful();
            }

            
            CTS.Cancel();
            stopping = false;
            stopped = true;

        }

        public static void Start() {


            if (configure.IsASource) {
                configure.TestSourceConfig();
            }
           
            if (configure.IsASubscriber) {   
                configure.TestSubscriberConfig();
            }
            
            if (configure.IsAPublisher) {
                configure.TestPublisherConfig();
            }
            
            
            

            stopped = false;

            


            RaiseOnStarted();
            CTS = new CancellationTokenSource();

            //get any stored config before starting
            ResolvePersistedConfig();

            if (configure.IsASource) {

                //if fewer publishers than max, only create threads for each pub.  otherwise, max 
                maxSourceThreads = configure.maxSourceThreads;// configure.Publishers.Count > configure.maxSourceThreads ? configure.maxSourceThreads : configure.Publishers.Count;
                sourceThreads = new List<Task>();
                sourceThreadsRunning = new ConcurrentDictionary<int, bool>();
                publishersRunning = new ConcurrentDictionary<string, pubStat>();

                

                for (int i = 1; i <= maxSourceThreads; i++) {
                    sourceThreads.Add(null);
                    sourceThreadsRunning.AddOrUpdate(i, true, (c, z) => { return true; });
                }

                configure.Publishers.ToList().ForEach(k => {
                    publishersRunning.AddOrUpdate(k.Key, new pubStat() { Has = true, Processing = false }, (c, z) => { return new pubStat() { Has = true, Processing = false }; });
                });

                //unlocks all messages that may have failed
                configure.source_LocalStorage.UnMarkAll();

                //restartTimer.AutoReset = false;
                //restartTimer.Interval = 5000;
                //restartTimer.Elapsed += RestartSource;

                StartProcessingSource();



            }
            if (configure.IsAPublisher) {
                //if fewer publishers than max, only create threads for each pub.  otherwise, max 
                maxPublisherThreads = configure.maxPublisherThreads;// configure.Publishers.Count > configure.maxSourceThreads ? configure.maxSourceThreads : configure.Publishers.Count;
                pubThreads = new List<Task>();
                pubThreadsRunning = new ConcurrentDictionary<int, bool>();
                subscribersRunning = new ConcurrentDictionary<string, pubStat>();


                


                for (int i = 1; i <= maxPublisherThreads; i++) {
                    pubThreads.Add(null);
                    pubThreadsRunning.AddOrUpdate(i, true, (c, z) => { return true; });
                }

                //add subscriptions for subscribe/unsubscribe
                ((IPublisherConfigure)configure).RegisterSubscriber("subsub", new SubscriptionSubscriber());
                ((IPublisherConfigure)configure).RegisterMessage<SubscriptionRequest>("subsub", false);

                ((IPublisherConfigure)configure).RegisterSubscriber("regsub", new EndpointRegistrationSubscriber());
                ((IPublisherConfigure)configure).RegisterMessage<EndpointRegistrationRequest>("regsub", false);

                configure.Subscribers.ToList().ForEach(k => {
                    subscribersRunning.AddOrUpdate(k.Key, new pubStat() { Has = true, Processing = false }, (c, z) => { return new pubStat() { Has = true, Processing = false }; });
                });

                //unlocks all messages that may have failed
                configure.publisher_LocalStorage.UnMarkAll();

                publisherRestartTimer.AutoReset = false;
                publisherRestartTimer.Interval = 1000;
                publisherRestartTimer.Enabled = true;
                publisherRestartTimer.Elapsed += RestartPublisher;

                //StartProcessingPublisher();
            }

            if (configure.IsASubscriber) {
                //if fewer publishers than max, only create threads for each pub.  otherwise, max 
                maxSubThreads = configure.MaxSubscriberThreads;// configure.Publishers.Count > configure.maxSourceThreads ? configure.maxSourceThreads : configure.Publishers.Count;
                subThreads = new List<Task>();
                subThreadsRunning = new ConcurrentDictionary<int, bool>();
                handlersRunning = new ConcurrentDictionary<string, pubStat>();

                for (int i = 1; i <= maxSubThreads; i++) {
                    subThreads.Add(null);
                    subThreadsRunning.AddOrUpdate(i, true, (c, z) => { return true; });
                }

                //submit subscription requests
                configure.Subscriptions.ForEach((s) => {

                    if (configure.Publishers.ContainsKey(s.EndPointName)) {
                        IEndPoint publisher = configure.Publishers[s.EndPointName];
                        if (!publisher.ServiceIsDown()) {
                            //??? how to do unsubscribe
                            SubscriptionRequest request = new SubscriptionRequest() {
                                EndPoint = configure.myEndPoint.EndPointAddress
                                , DiscardIfSubscriberIsDown = s.DiscardIfDown
                                , GUID = Util.Util.GetApplicationGuid()
                                , MessageTypeName = s.TypeName
                                , Name = ApplicationName
                                , DeRegister = false
                            };
                            string payLoad = JsonConvert.SerializeObject(request);
                            PersistedMessage msg = new PersistedMessage() {
                                 HandleRetryCount = 0
                                , Headers = new Dictionary<string, string>()
                                , MessageType = Util.Util.GetTypeName(typeof(SubscriptionRequest))
                                , Ordinal = 0
                                , PayLoad = payLoad
                                , Publisher = s.EndPointName
                                , SendRetryCount = 0
                                , Status =  PersistedMessageStatusOptions.ReadyToProcess
                            };
                            publisher.Publish(msg);
                        }
                    }


                });

    
                configure.Handlers.ToList().ForEach(k => {
                    handlersRunning.AddOrUpdate(k.Key, new pubStat() { Has = true, Processing = false }, (c, z) => { return new pubStat() { Has = true, Processing = false }; });
                });

                configure.subscriber_LocalStorage.UnMarkAll();

                subscriberRestartTimer = new System.Timers.Timer();
                subscriberRestartTimer.AutoReset = false;
                subscriberRestartTimer.Interval = 5000;
                subscriberRestartTimer.Enabled = true;
                subscriberRestartTimer.Elapsed += RestartSubscriber;

                //StartProcessingSubscriber();
            }



        }

        private static void RestartSource(object sender, System.Timers.ElapsedEventArgs e) {

            StartProcessingSource();

        }
        private static void RestartPublisher(object sender, System.Timers.ElapsedEventArgs e) {
            StartProcessingPublisher();
        }
        private static void RestartSubscriber(object sender, System.Timers.ElapsedEventArgs e) {
            StartProcessingSubscriber();
        }

        #region Source

        private static AutoResetEvent sourceRE = new AutoResetEvent(false);
        private static List<Task> sourceThreads = null;       
        private static ConcurrentDictionary<int,bool> sourceThreadsRunning = null;
        private static ConcurrentDictionary<string, pubStat> publishersRunning = null;
        private static int currentPublisher = 0;
        private static int maxSourceThreads = 1;

        public static string SendMessage<T>(T message) {



            if (stopped || stopping) {
                throw new BusNotRunningException("Bus has been stopped and cannot accept any new messages");
            }

            if (!configure.IsASource) {
                throw new BusNotConfiguredException("Source", "Bus not configured as a source");
            }

            //serialize message
            string serialized = JsonConvert.SerializeObject(message, new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.All });


            //create peristed message
            PersistedMessage msg = new PersistedMessage(serialized);

            JObject asJO = JObject.Parse(serialized);
            msg.MessageType = asJO["$type"].ToString();


            //persist message to local storage
            try {

                int id = SyncGetPadding();

                var publishers = configure.Messages.Where(g => g.TypeName.Equals(msg.MessageType, StringComparison.OrdinalIgnoreCase));
                List<PersistedMessage> messages = new List<PersistedMessage>();

                foreach (MessageTypeMapping m in publishers) {
                    PersistedMessage copied = new PersistedMessage() {
                        HandleRetryCount = msg.HandleRetryCount
                        , PayLoad = msg.PayLoad
                        , Headers = msg.Headers
                        , MessageType = msg.MessageType
                        , Published = null
                        , Publisher = m.EndPointName
                        , SendRetryCount = msg.SendRetryCount
                        , Sent = DateTime.UtcNow
                        , Status = PersistedMessageStatusOptions.ReadyToProcess
                        , Subscriber = null
                        , Ordinal = id
                        , TransactionID = msg.TransactionID
                        , Queue = m.EndPointName
                    };
                    messages.Add(copied);
                }

                if (configure.source_LocalStorage.ServiceIsDown()) {
                    throw new ServiceEndpointDownException("Message Peristor is Down");
                }

                Task whenDone = new Task((p) => {
                    IEnumerable<MessageTypeMapping> pubs = ((IEnumerable<MessageTypeMapping>)p);

                    foreach (MessageTypeMapping m in pubs) {
                        publishersRunning.AddOrUpdate(m.EndPointName, new pubStat() { Has = true, Processing = false }, (k, v) => {

                            if (!v.Processing) {
                                return new pubStat() { Has = true, Processing = false };
                            } else {
                                return v;
                            }
                        });
                    }

                    StartProcessingSource();

                }, publishers);

                TxRM rm = new TxRM();
                
                string transactionId = rm.SendMessage(messages, configure.source_LocalStorage, whenDone);



                //foreach (MessageTypeMapping m in publishers) {
                //    publishersRunning.AddOrUpdate(m.EndPointName, new pubStat() { Has = true, Processing = false }, (k, v) => {

                //        if (!v.Processing) {
                //            return new pubStat() { Has = true, Processing = false };
                //        } else {
                //            return v;
                //        }
                //    });
                //}

                return transactionId;


            } catch (Exception) {
                throw;
            }

            


        }

        public static void SendMessage<T>(T Message, Dictionary<string, string> Headers) {
            throw new NotImplementedException();
        }

        private static void StopProcessingSource(int threadId) {

            //if (!sourceCTS.IsCancellationRequested) { sourceCTS.Cancel(); }
            sourceThreadsRunning.AddOrUpdate(threadId, false, (k, v) => {
                return false;
            });

            Stall(threadId);
        }

        private static void StartProcessingSource() {


            for (int i = 1; i <= maxSourceThreads; i++) {

                Task t = sourceThreads[i - 1];
                if (t == null || t.IsCompleted) {
                    //t = new Thread(PublishNext);

                    //capture i during the loop, since publishnext is running from the facotry, it may begin
                    //after i has been incremeneted.
                    int q = i;

                    t = Task.Factory.StartNew(() => {
                        PublishNext(q);
                    }, CTS.Token,  TaskCreationOptions.LongRunning, TaskScheduler.Default );

                    sourceThreadsRunning.AddOrUpdate(i, true, (k, v) => { return true; });
                    
                   sourceThreads[i - 1] = t;
                }


                
            }

            //trigger threads to start waiting
            sourceRE.Set();
            RaiseOnProcessing();

        }
    
        private static void PublishNext(int threadId) {


            RaiseOnThreadStarted();

            //Console.WriteLine("Thread {0} Waiting", myThreadId);
            bool process = true;
    
            while (process && !CTS.IsCancellationRequested) {
                if (sourceRE.WaitOne()) {

                    //shut down loop unless we have a publisher to query
                    process = false;
                    if (!CTS.IsCancellationRequested) {


                        //Console.WriteLine("Thread {0} going", myThreadId);
                        int newThreadId = threadId + 1;
                        if (newThreadId == (maxSourceThreads + 1)) { newThreadId = 1; }

                        //if current thread is mine, proceed and set to next thread
                        //if (threadId == Interlocked.CompareExchange(ref maxSourceThreads, threadId, newThreadId)) {

                        //determine what publisher to use.

                        string q = SyncGetNextPublisherWithMessages();
                        if (!string.IsNullOrEmpty(q)) {


                            process = true;
                            //get next message to publish
                            PersistedMessage msg = SyncGetMessageToProcess(q, configure.source_LocalStorage);

                            //if we found a message, reset so next thread can look
                            //otherwise, don't reset... this will effectively stop all threads until soemthing else
                            //notifies that there is something to process.
                            if (msg != null) {

                                //we have our value, so pass on to next thread
                                sourceRE.Set();



                                //publish message
                                bool published = PublishMessage(msg);

                                if (!configure.source_LocalStorage.ServiceIsDown()) {
                                    try {
                                        if (published) {
                                            //if localstorage experiences an error, it will shut itself down
                                            var popped = configure.source_LocalStorage.Pop(msg.Id);
                                        }

                                        publishersRunning.AddOrUpdate(q, new pubStat() { Has = true, Processing = false }, (k, v) => {
                                            return new pubStat() { Has = true, Processing = false };
                                        });

                                    } catch (Exception) {
                                        //don't need to do anything but stop the process
                                        //the message will be left in a transitory state, but will be retained.
                                        process = false;
                                    }
                                }

                            } else {
                                //if we didn't find a message, stop processing that q until another comes in

                                publishersRunning.AddOrUpdate(q, new pubStat() { Has = false, Processing = false }, (k, v) => {
                                    return new pubStat() { Has = false, Processing = false };
                                });

                                sourceRE.Set();
                            }

                        } else {
                            //if we don't have a q to process, allow the next thread to do its 
                            //work so it can shut itself down, too
                            process = false;
                            sourceRE.Set();
                        }


                        //} else {

                        //    process = true;
                        //    //err, not my turn, pass on to next.
                        //    sourceRE.Set();

                        //}
                    }
                }

   

            } // end while cancellation token
            StopProcessingSource(threadId);

        }

        private static bool PublishMessage(PersistedMessage msg) {

            bool toReturn = false;
            try { 
            var publishers = from c in configure.Messages where c.TypeName.Equals(msg.MessageType, StringComparison.OrdinalIgnoreCase) && c.EndPointName == msg.Publisher select c;

                foreach (var entry in publishers) {

                    IEndPoint pub = configure.Publishers.FirstOrDefault(g => g.Key.Equals(entry.EndPointName, StringComparison.OrdinalIgnoreCase)).Value;
                    if (pub != null && !pub.ServiceIsDown()) {

                        try {

                            toReturn = pub.Publish(msg).Status;
                        } catch {
                            toReturn = entry.DiscardIfDown;
                        }
                    } else if (pub.ServiceIsDown()) {
                        //if subscription allows discard, just return true and let process discard message
                        toReturn = entry.DiscardIfDown;
                            
                    }
                }
            } catch (Exception) {
                toReturn = false;
            }
            return toReturn;

        }

        private static string SyncGetNextPublisherWithMessages() {

            string toReturn = null;
            lock (sourceLock) {

                int current = currentPublisher;
                
                int next = current + 1;
                //a thread is already using current, so loop through remaining and grab next
                for (int i = 0; i < (publishersRunning.Count); i++) {

                    if (next >= publishersRunning.Count) { next = 0; }
                    string q = publishersRunning.ToList()[next].Key;
                    pubStat stat = publishersRunning[q];
                    if (stat.Has && !stat.Processing) {
                        toReturn = q;
                        publishersRunning.AddOrUpdate(q, new pubStat() { Has = true, Processing = true }, (k, v) => {
                            return new pubStat() { Has = true, Processing = true };
                        });
                        currentPublisher = next;
                        break;
                    }
                    next++;
                }
                if (string.IsNullOrEmpty(toReturn)) {
                    currentPublisher = next - 1;
                    if (currentPublisher < 0) {
                        currentPublisher = publishersRunning.Count - 1;
                    }
                }
                

            }
            return toReturn;
        }


        #endregion

        #region Publisher

        private static AutoResetEvent pubRE = new AutoResetEvent(false);
        private static List<Task> pubThreads = null;
        private static ConcurrentDictionary<int, bool> pubThreadsRunning = null;
        private static ConcurrentDictionary<string, pubStat> subscribersRunning = null;
        private static int currentSubscriber = 0;
        private static int maxPublisherThreads = 1;

        public static void BroadcastMessage(PersistedMessage message) {

            if (stopped || stopping) {
                throw new BusNotRunningException("Bus has been stopped and cannot accept any new messages");
            }

            if (!configure.IsAPublisher) {
                throw new BusNotConfiguredException("Publisher", "Bus not configured as a Publisher");
            }


            //persist message to local storage
            try {

                int id = SyncGetPadding();

                var subscribers = configure.Subscriptions.Where(g => g.TypeName.Equals(message.MessageType, StringComparison.OrdinalIgnoreCase));
                List<PersistedMessage> messages = new List<PersistedMessage>();

                foreach (MessageTypeMapping m in subscribers) {
                    PersistedMessage copied = (PersistedMessage)message.Clone();
                    copied.Subscriber = m.EndPointName;
                    copied.Queue = m.EndPointName;
                    copied.Published = DateTime.UtcNow;
                    copied.Ordinal = id;
                    copied.Status = PersistedMessageStatusOptions.ReadyToProcess;
                    messages.Add(copied);
          
                }

                if (configure.publisher_LocalStorage.ServiceIsDown()) {
                    throw new ServiceEndpointDownException("Message Peristor is Down");
                }

                //only persist if somebody is listening to it
                if (messages.Count() > 0) {
                    configure.publisher_LocalStorage.Persist(messages);
                    foreach (MessageTypeMapping m in subscribers) {
                        subscribersRunning.AddOrUpdate(m.EndPointName, new pubStat() { Has = true, Processing = false }, (k, v) => {

                            if (!v.Processing) {
                                return new pubStat() { Has = true, Processing = false };
                            } else {
                                return v;
                            }
                        });
                    }
                }


            } catch (Exception) {
                throw;
            }

            StartProcessingPublisher();
        }

        public static void ReceiveMessage(PersistedMessage message) {



            if (stopped || stopping) {
                throw new BusNotRunningException("Bus has been stopped and cannot accept any new messages");
            }

            if (!configure.IsASubscriber) {
                throw new BusNotConfiguredException("Subscriber", "Bus not configured as a subscriber");
            }




            //persist message to local storage
            try {

                int id = SyncGetPadding();

                var handlers = configure.Handlers.Where(g => g.Key.Equals(message.MessageType, StringComparison.OrdinalIgnoreCase));
                List<PersistedMessage> messages = new List<PersistedMessage>();

                foreach (KeyValuePair<string, IMessageHandler> m in handlers) {
                    PersistedMessage copied = (PersistedMessage)message.Clone();
                    copied.MessageHandler = m.Key;
                    copied.Queue = m.Key;
                    copied.Received = DateTime.UtcNow;
                    copied.Ordinal = id;
                    copied.Status = PersistedMessageStatusOptions.ReadyToProcess;
                    messages.Add(copied);
                }

                if (configure.subscriber_LocalStorage.ServiceIsDown()) {
                    throw new ServiceEndpointDownException("Message Peristor is Down");
                }

                configure.subscriber_LocalStorage.Persist(messages);
                foreach (KeyValuePair<string, IMessageHandler> m in handlers) {
                    handlersRunning.AddOrUpdate(m.Key, new pubStat() { Has = true, Processing = false }, (k, v) => {

                        if (!v.Processing) {
                            return new pubStat() { Has = true, Processing = false };
                        } else {
                            return v;
                        }
                    });
                }


            } catch (Exception) {
                throw;
            }

            StartProcessingSubscriber();


        }

        private static void StartProcessingPublisher() {
            for (int i = 1; i <= maxPublisherThreads; i++) {

                Task t = pubThreads[i - 1];
                if (t == null || t.IsCompleted) {
                    //t = new Thread(PublishNext);

                    //capture i during the loop, since publishnext is running from the facotry, it may begin
                    //after i has been incremeneted.
                    int q = i;

                    t = Task.Factory.StartNew(() => {
                        DistributeNext(q);
                    }, CTS.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

                    pubThreadsRunning.AddOrUpdate(i, true, (k, v) => { return true; });

                    pubThreads[i - 1] = t;
                }



            }

            //trigger threads to start waiting
            pubRE.Set();
            RaiseOnProcessing();
        }

        private static void DistributeNext(int threadId) {

            RaiseOnThreadStarted();

            //Console.WriteLine("Thread {0} Waiting", myThreadId);
            bool process = true;

            while (process && !CTS.IsCancellationRequested) {
                if (pubRE.WaitOne()) {

                    //shut down loop unless we have a publisher to query
                    process = false;
                    if (!CTS.IsCancellationRequested) {


                        //Console.WriteLine("Thread {0} going", myThreadId);
                        //int newThreadId = threadId + 1;
                        //if (newThreadId == (maxSourceThreads + 1)) { newThreadId = 1; }

                        //if current thread is mine, proceed and set to next thread
                        //if (threadId == Interlocked.CompareExchange(ref maxSourceThreads, threadId, newThreadId)) {

                        //determine what publisher to use.

                        string q = SyncGetNextSubscriberWithMessages();
                        if (!string.IsNullOrEmpty(q)) {


                            process = true;
                            //get next message to publish
                            PersistedMessage msg = SyncGetMessageToProcess(q, configure.publisher_LocalStorage);

                            //if we found a message, reset so next thread can look
                            //otherwise, don't reset... this will effectively stop all threads until soemthing else
                            //notifies that there is something to process.
                            if (msg != null) {

                                //we have our value, so pass on to next thread
                                pubRE.Set();



                                //publish message
                                bool distributed = DistributeMessage(msg);

                                if (!configure.publisher_LocalStorage.ServiceIsDown()) {
                                    try {

                                        if (distributed) { 
                                        //if localstorage experiences an error, it will shut itself down
                                            var popped = configure.publisher_LocalStorage.Pop(msg.Id);
                                        }

                                        subscribersRunning.AddOrUpdate(q, new pubStat() { Has = true, Processing = false }, (k, v) => {
                                            return new pubStat() { Has = true, Processing = false };
                                        });

                                    } catch (Exception) {
                                        //don't need to do anything but stop the process
                                        //the message will be left in a transitory state, but will be retained.
                                        process = false;
                                    }
                                }

                            } else {
                                //if we didn't find a message, stop processing that q until another comes in

                                subscribersRunning.AddOrUpdate(q, new pubStat() { Has = false, Processing = false }, (k, v) => {
                                    return new pubStat() { Has = false, Processing = false };
                                });

                                pubRE.Set();
                            }

                        } else {
                            //if we don't have a q to process, allow the next thread to do its 
                            //work so it can shut itself down, too
                            process = false;
                            pubRE.Set();
                        }


                        //} else {

                        //    process = true;
                        //    //err, not my turn, pass on to next.
                        //    sourceRE.Set();

                        //}
                    }
                }



            } // end while cancellation token
            StopProcessingPublisher(threadId);
        }

        private static void StopProcessingPublisher(int threadId) {
            pubThreadsRunning.AddOrUpdate(threadId, false, (k, v) => { return false; });



            Stall(threadId);
        }

        private static bool DistributeMessage(PersistedMessage msg) {

            //if there are not any subscribers, just let the process remove the message
            bool toReturn = false;

            var subscriptions = from c in configure.Subscriptions where c.TypeName.Equals(msg.MessageType, StringComparison.OrdinalIgnoreCase) select c;

            //subscripions may be modified here, so execute linq immediately 
            var toProcess = new List<MessageTypeMapping>(subscriptions);

            foreach (var entry in toProcess) {

                IEndPoint subscriber = configure.Subscribers.FirstOrDefault(g => g.Key.Equals(entry.EndPointName, StringComparison.OrdinalIgnoreCase)).Value;
                toReturn = subscriber == null;

                if (subscriber != null && !subscriber.ServiceIsDown()) {
                    EndpointResponse response = subscriber.Publish(msg);
                    toReturn = response.Status;
                } 
            }

            return toReturn;
        }

        private static string SyncGetNextSubscriberWithMessages() {
            string toReturn = null;
            lock (sourceLock) {

                int current = currentSubscriber;
                bool processSubscriber = true;

                //if the subscription subscriber has messages, we need to process those immediately
                //this ensures that the subscriber gets all messsages from the time it subscribes
                pubStat s = subscribersRunning["subsub"];
                if (s.Has && !s.Processing) { // if message on queue, but not processing... process it immediately
                    toReturn = "subsub";
                    subscribersRunning.AddOrUpdate("subsub", new pubStat() { Has = true, Processing = true }, (k, v) => {
                        return new pubStat() { Has = true, Processing = true };
                    });
                    processSubscriber = false;
                } else if (s.Has && s.Processing) { //if message on queue, but already processing, don't process anything else until queue is done

                    //if we are processing subscription requests, stop all processing until done.

                    toReturn = null;
                    processSubscriber = false;
                }

        

                if (processSubscriber) {
                    int next = current + 1;
                    //a thread is already using current, so loop through remaining and grab next
                    for (int i = 0; i < (subscribersRunning.Count); i++) {

                        if (next >= subscribersRunning.Count) { next = 0; }
                        string q = subscribersRunning.ToList()[next].Key;
                        pubStat stat = subscribersRunning[q];
                        if (stat.Has && !stat.Processing) {
                            toReturn = q;
                            subscribersRunning.AddOrUpdate(q, new pubStat() { Has = true, Processing = true }, (k, v) => {
                                return new pubStat() { Has = true, Processing = true };
                            });
                            currentSubscriber = next;
                            break;
                        }
                        next++;
                    }

                    if (string.IsNullOrEmpty(toReturn)) {
                        currentSubscriber = next - 1;
                        if (currentSubscriber < 0) {
                            currentSubscriber = subscribersRunning.Count - 1;
                        }
                    }

                }

            }
            return toReturn;
        }

        #endregion

        #region Subscriber

        private static AutoResetEvent subRE = new AutoResetEvent(false);
        private static List<Task> subThreads = null;
        private static ConcurrentDictionary<int, bool> subThreadsRunning = null;
        private static ConcurrentDictionary<string, pubStat> handlersRunning = null;
        private static int currentHandler = 0;
        private static int maxSubThreads = 1;




        private static void StopProcessingSubscriber(int threadId) {

            //if (!sourceCTS.IsCancellationRequested) { sourceCTS.Cancel(); }
            subThreadsRunning.AddOrUpdate(threadId, false, (k, v) => {
                return false;
            });

            Stall(threadId);
        }

        private static void StartProcessingSubscriber() {


            for (int i = 1; i <= maxSubThreads; i++) {

                Task t = subThreads[i - 1];
                if (t == null || t.IsCompleted) {
                    //t = new Thread(PublishNext);

                    //capture i during the loop, since publishnext is running from the facotry, it may begin
                    //after i has been incremeneted.
                    int q = i;

                    t = Task.Factory.StartNew(() => {
                        HandleNext(q);
                    }, CTS.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

                    subThreadsRunning.AddOrUpdate(i, true, (k, v) => { return true; });

                    subThreads[i - 1] = t;
                }



            }

            //trigger threads to start waiting
            subRE.Set();
            RaiseOnProcessing();

        }

        private static void HandleNext(int threadId) {


            RaiseOnThreadStarted();

            //Console.WriteLine("Thread {0} Waiting", myThreadId);
            bool process = true;

            while (process && !CTS.IsCancellationRequested) {
                if (subRE.WaitOne()) {

                    //shut down loop unless we have a publisher to query
                    process = false;
                    if (!CTS.IsCancellationRequested) {


                        //Console.WriteLine("Thread {0} going", myThreadId);
                        int newThreadId = threadId + 1;
                        if (newThreadId == (maxSubThreads + 1)) { newThreadId = 1; }

                        //if current thread is mine, proceed and set to next thread
                        //if (threadId == Interlocked.CompareExchange(ref maxSourceThreads, threadId, newThreadId)) {

                        //determine what publisher to use.

                        string q = SyncGetNextHandlerWithMessages();
                        if (!string.IsNullOrEmpty(q)) {


                            process = true;
                            //get next message to publish
                            PersistedMessage msg = SyncGetMessageToProcess(q, configure.subscriber_LocalStorage);

                            //if we found a message, reset so next thread can look
                            //otherwise, don't reset... this will effectively stop all threads until soemthing else
                            //notifies that there is something to process.
                            if (msg != null) {

                                configure.subscriber_LocalStorage.Processing(msg.Id);
                                //we have our value, so pass on to next thread
                                subRE.Set();



                                //publish message
                                HandlerResponse handled = HandleMessage(msg);

                                if (!configure.subscriber_LocalStorage.ServiceIsDown()) {
                                    try {
                                        if (handled.Status == HandlerStatusOptions.Handled) {
                                            //if localstorage experiences an error, it will shut itself down
                                            var popped = configure.subscriber_LocalStorage.Pop(msg.Id);
                                        } else if (handled.Status == HandlerStatusOptions.Reschedule ) {

                                            //need to update message as rescheduled with new date
                                            var popped = configure.subscriber_LocalStorage.Reschedule(msg.Id, handled.RescheduleIncrement);

                                        }

                                        handlersRunning.AddOrUpdate(q, new pubStat() { Has = true, Processing = false }, (k, v) => {
                                            return new pubStat() { Has = true, Processing = false };
                                        });

                                    } catch (Exception) {
                                        //don't need to do anything but stop the process
                                        //the message will be left in a transitory state, but will be retained.
                                        process = false;
                                    }
                                }

                            } else {
                                //if we didn't find a message, stop processing that q until another comes in

                                handlersRunning.AddOrUpdate(q, new pubStat() { Has = false, Processing = false }, (k, v) => {
                                    return new pubStat() { Has = false, Processing = false };
                                });

                                subRE.Set();
                            }

                        } else {
                            //if we don't have a q to process, allow the next thread to do its 
                            //work so it can shut itself down, too
                            process = false;
                            subRE.Set();
                        }


                        //} else {

                        //    process = true;
                        //    //err, not my turn, pass on to next.
                        //    sourceRE.Set();

                        //}
                    }
                }



            } // end while cancellation token
            StopProcessingSubscriber(threadId);

        }

        private static HandlerResponse HandleMessage(PersistedMessage msg) {

            HandlerResponse toReturn = null;
            try {
                var handlers = from c in configure.Handlers where c.Key.Equals(msg.MessageType, StringComparison.OrdinalIgnoreCase) select c;

                foreach (var entry in handlers) {

                    toReturn = entry.Value.Handle(msg);
                }
                


            } catch (Exception) {
                toReturn = HandlerResponse.Exception();
            }

            if (toReturn == null) { toReturn = HandlerResponse.Exception(); }

            return toReturn;

        }

        private static string SyncGetNextHandlerWithMessages() {

            string toReturn = null;
            lock (sourceLock) {

                int current = currentHandler;

                int next = current + 1;
                //a thread is already using current, so loop through remaining and grab next
                for (int i = 0; i < (handlersRunning.Count); i++) {

                    if (next >= handlersRunning.Count) { next = 0; }
                    string q = handlersRunning.ToList()[next].Key;
                    pubStat stat = handlersRunning[q];

                    IMessageHandler handler = configure.Handlers[q];

                    if (stat.Has && (!stat.Processing || handler.Parallel)) {
                        toReturn = q;
                        handlersRunning.AddOrUpdate(q, new pubStat() { Has = true, Processing = !handler.Parallel }, (k, v) => {
                            return new pubStat() { Has = true, Processing = !handler.Parallel };
                        });
                        currentHandler = next;
                        break;
                    }
                    next++;
                }
                if (string.IsNullOrEmpty(toReturn)) {
                    currentHandler = next - 1;
                    if (currentHandler < 0) {
                        currentHandler = handlersRunning.Count - 1;
                    }
                }


            }
            return toReturn;
        }


        #endregion



        #region Shared
        private static PersistedMessage SyncGetMessageToProcess(string q, IPersist storage) {

            PersistedMessage toReturn = null;
            lock (sourceLock) {

                if (!storage.ServiceIsDown()) {
                    //get the next one from the database
                    try {

                        if (stopped || stopping) {
                            toReturn = null;
                        } else {

                            toReturn = storage.PeekAndMarkNext(q);
                        }
                    } catch (Exception) {
                        toReturn = null;
                    }

                } else {
                    toReturn = null;
                }

            }
            return toReturn;
        }

        #endregion

    }
}
