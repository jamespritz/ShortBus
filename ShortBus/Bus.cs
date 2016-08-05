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
using ShortBus.Routing;

namespace ShortBus {

    class TxRM : IEnlistmentNotification {

        public TxRM() { }

        private IEnumerable<PersistedMessage> messages = null;
        private IPersist persistor = null;
        private string txId = string.Empty;
        private Action whenDone = null;

        public string SendMessage(IEnumerable<PersistedMessage> messages, IPersist persistor, Action whenDone) {

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
                whenDone.Invoke();
            }

            return txId;

        }

        void IEnlistmentNotification.Commit(Enlistment enlistment) {
            persistor.CommitBatch(this.txId);
            enlistment.Done();
            whenDone.Invoke();
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

      
        // TODO - Define Event Args for Bus Events, possibly create more specic events
        public delegate void BusStarted(object sender, EventArgs args);
        public static event BusStarted OnStarted;
        public delegate void BusProcessing(object sender, EventArgs args);
        public static event BusProcessing OnProcessing;
        public delegate void ThreadStalled(object sender, EventArgs args);
        public static event ThreadStalled OnStalled;
        public delegate void ThreadStarted(object sender, EventArgs args);
        public static event ThreadStarted OnThreadStarted;


        private static System.Timers.Timer restartTimer = new System.Timers.Timer();
        

        private static int maxThreads = 1;

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



        class EndpointRegistrationHandler : IMessageHandler {
            EndPointTypeOptions IEndPoint.EndPointType {
                get {
                    return EndPointTypeOptions.Handler;
                }
            }

            bool IMessageHandler.Parallel {
                get {
                    return false;
                }
            }

            HandlerResponse IMessageHandler.Handle(PersistedMessage message) {

                
                string payLoad = message.PayLoad;
                EndpointRegistrationRequest request = Newtonsoft.Json.JsonConvert.DeserializeObject<EndpointRegistrationRequest>(payLoad);

                EndpointConfigPersist<EndPointConfig> config = new EndpointConfigPersist<EndPointConfig>(configure.Persitor);
                EndPointConfig stored = config.GetConfig();

                bool changed = false;

                //check if we know about the endpoint
                if (stored.EndPoints.Count(g => g.Name.Equals(request.Name, StringComparison.OrdinalIgnoreCase)) == 0) {
                    stored.EndPoints.Add(new EndPoint() { Name = request.Name, EndPointAddress = request.EndPoint, EndPointType = request.EndPointType });
                    changed = true;
                }

                List<MessageTypeMapping> newRoutes = new List<MessageTypeMapping>();
                List<MessageTypeMapping> changedRoutes = new List<MessageTypeMapping>();
                //if endpoint has subscriptions, make sure they are in my config so I don't forget about them
                request.SubscriptionRequests.ForEach(r => {

                    var storedSubscription = stored.Subscriptions.FirstOrDefault(s => s.TypeName.Equals(r.MessageTypeName, StringComparison.OrdinalIgnoreCase));
                    if (storedSubscription == null) {
                        newRoutes.Add(new MessageTypeMapping(r.MessageTypeName, request.Name, MessageDirectionOptions.Outbound, r.DiscardIfSubscriberIsDown));
                    } else {
                        if (storedSubscription.DiscardIfDown != r.DiscardIfSubscriberIsDown) {
                            storedSubscription.DiscardIfDown = r.DiscardIfSubscriberIsDown;
                            changedRoutes.Add(new MessageTypeMapping(r.MessageTypeName, request.Name, MessageDirectionOptions.Outbound, r.DiscardIfSubscriberIsDown));
                            changed = true;
                        }
                    }

                });
                if (newRoutes.Count() > 0) {
                    stored.Subscriptions.AddRange(from r in newRoutes select r);
                    changed = true;
                }

                if (changed) {
                    
                    configure.EndPoints.TryAdd(request.Name, new RESTEndPoint(new RESTSettings(request.EndPoint, request.EndPointType)));


                    newRoutes.ForEach(r => {
                       ((IConfigureInternal)configure).RouteMessage(r.TypeName, r.EndPointName, r.DiscardIfDown);
                    });
                    changedRoutes.ForEach(r => {
                        MessageTypeMapping existing = configure.Routes.FirstOrDefault(
                            g => g.EndPointName.Equals(r.EndPointName, StringComparison.OrdinalIgnoreCase) &&
                                g.TypeName.Equals(r.TypeName, StringComparison.OrdinalIgnoreCase)
                            );
                        if (existing != null) {
                            existing.DiscardIfDown = r.DiscardIfDown;
                        }

                    });

                    //make sure endpointsrunning has an entry for every routed endpoint
                    var activeEndpoints = from r in configure.Routes group r by r.EndPointName into grouped
                                          select new { EP = grouped.Key };
                    activeEndpoints.ToList().ForEach(g => {
                        endPointsRunning.AddOrUpdate(g.EP, new pubStat() { Has = true, Processing = false }
                        , (r, z) => { return new pubStat() { Has = true, Processing = false }; });

                    });

                    config.UpdateConfig(stored);

                    
                }
                IMessageRouter router =  (IMessageRouter)configure.EndPoints[request.Name];
                if (router.ServiceIsDown()) {
                    router.ResetConnection(request.EndPoint);
                    configure.Persitor.ToggleMarkAll(request.Name, PersistedMessageStatusOptions.ReadyToProcess, PersistedMessageStatusOptions.Marked);
                    endPointsRunning.AddOrUpdate(request.Name, new pubStat() { Has = true, Processing = false }, (c, z) => { return new pubStat() { Has = true, Processing = false }; });

                }

                StartProcessing();
                return HandlerResponse.Handled();
            }

        }//end handler



        //all messages are processed by incoming date/time.  Since it may be
        //possible that two messages have same date/time, each messages is given
        //an ordinal when first persisted. Bus will sort by date/time and ordinal to
        //ensure messages are processed in the order they were taken.
        //ordinal is reset to zero if more than 5 seconds have passed.
        private static int ordinal = 0;
        private static DateTime padTime = DateTime.Now;
        private static int SyncGetPadding() {

            lock (lockObj) {
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
        private readonly static object lockObj = new object();
        private static CancellationTokenSource CTS = null;
        private static bool stopped = true;
        private static bool stopping = false;
        private static ManualResetEvent allStopped = new ManualResetEvent(true);


        private static AutoResetEvent resetEvent = new AutoResetEvent(false);
        private static List<Task> taskThreads = null;
        private static ConcurrentDictionary<int, bool> taskThreadsRunning = null;
        private static ConcurrentDictionary<string, pubStat> endPointsRunning = null;
        private static int currentEndpoint = 0;



        private static void ResolvePersistedConfig() {
            
                
                EndPointConfig stored = null;
                EndpointConfigPersist<EndPointConfig> config = new EndpointConfigPersist<EndPointConfig>(configure.Persitor);
                

                stored = config.GetConfig();
                if (stored == null) {
                    stored = new EndPointConfig() {
                        ApplicationGUID = ((IConfigure)configure).ApplicationGUID
                        , ApplicationName = ((IConfigure)configure).ApplicationName
                        , Version = "1.0"
                        , EndPoints = new List<EndPoint>()
                        , Subscriptions = new List<MessageTypeMapping>()
                    };
                    config.UpdateConfig(stored);
                }

   


                foreach (EndPoint s in stored.Subscribers()) {
                    ((IConfigure)configure).RegisterEndpoint(s.Name, new RESTEndPoint(new RESTSettings(s.EndPointAddress, s.EndPointType )));

                }
                foreach (MessageTypeMapping map in stored.Subscriptions) {
                    ((IConfigureInternal)configure).RouteMessage(map.TypeName, map.EndPointName, map.DiscardIfDown);
                }



            
        }

        private static bool AllThreadsAreStopped() {

            bool allThreadsAreStopped = true;
            if (taskThreadsRunning != null && taskThreadsRunning.Count() > 0) {
                allThreadsAreStopped = allThreadsAreStopped && (!taskThreadsRunning.Any(g => g.Value));

            }

            return allThreadsAreStopped;
        }

        private static void Stall(int threadId) {

            
            if (AllThreadsAreStopped()) {
                
                if (stopping) {
                    allStopped.Set();
                } else { //if we are stopping the bus, don't restart the threads... that would be silly!

                    //otherwise, start the restart timer 
                    if (restartTimer != null) {
                        restartTimer.Start();
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


        // TODO: fix subscription request
        // needs to include subscriptions
        private static void PingEndPoints() {

            //if i am a subscriber, find any routes where endpoint is a router... for each route, subscribe to message.

            //submit subscription requests
            configure.Routes.ForEach((s) => {

                if (configure.EndPoints.ContainsKey(s.EndPointName)) {
                    IEndPoint endpoint = configure.EndPoints[s.EndPointName];

                    if (typeof(IMessageRouter).IsAssignableFrom(endpoint.GetType())) {

                        IMessageRouter router = (IMessageRouter)endpoint;
                        if (!router.ServiceIsDown()) {
                            //??? how to do unsubscribe
                            EndpointRegistrationRequest request = new EndpointRegistrationRequest() {
                                EndPoint = configure.myEndPoint.EndPointAddress
                                , GUID = Util.Util.GetApplicationGuid()
                                , Name = ApplicationName
                                , EndPointType = configure.myEndPoint.EndPointType
                                , DeRegister = false
                            };
                            string payLoad = JsonConvert.SerializeObject(request);
                            PersistedMessage msg = new PersistedMessage() {

                                Headers = new Dictionary<string, string>()
                                , MessageType = Util.Util.GetTypeName(typeof(SubscriptionRequest))
                                , Ordinal = 0
                                , DateStamp = DateTime.UtcNow
                                , PayLoad = payLoad
                                , Status = PersistedMessageStatusOptions.ReadyToProcess
                            };

                            router.Publish(msg);
                        }
                    }
                }


            });
        }

        public static void Start() {


            try {
                configure.TestConfig();


                maxThreads = configure.MaxThreads;// configure.Publishers.Count > configure.maxSourceThreads ? configure.maxSourceThreads : configure.Publishers.Count;

                stopped = false;




                RaiseOnStarted();
                CTS = new CancellationTokenSource();

                //get any stored config before starting
                ResolvePersistedConfig();



                taskThreads = new List<Task>();
                taskThreadsRunning = new ConcurrentDictionary<int, bool>();
                for (int i = 1; i <= maxThreads; i++) {
                    taskThreads.Add(null);
                    taskThreadsRunning.AddOrUpdate(i, true, (c, z) => { return true; });
                }


                endPointsRunning = new ConcurrentDictionary<string, pubStat>();


                ((IConfigure)configure).RegisterMessageHandler<EndpointRegistrationRequest>(new EndpointRegistrationHandler());

                var subscriptions = (from r in configure.Routes
                                     where r.Direction == MessageDirectionOptions.Inbound
                                     group r by r.EndPointName into grouped
                                     select new { EndPointName = grouped.Key, Routes = grouped });
                foreach (var endPoint in subscriptions) {

                    EndpointRegistrationRequest request = new EndpointRegistrationRequest() {
                        EndPoint = configure.myEndPoint.EndPointAddress
                        , EndPointType = configure.myEndPoint.EndPointType
                        , GUID = configure.ApplicationGUID
                        , Name = configure.myEndPoint.Name
                        , SubscriptionRequests = (from r in endPoint.Routes select new SubscriptionRequest() {
                            DiscardIfSubscriberIsDown = r.DiscardIfDown
                            , MessageTypeName = r.TypeName
                        }).ToList()
                    };
                    string payLoad = JsonConvert.SerializeObject(request);
                    PersistedMessage msg = new PersistedMessage() {
                        Headers = new Dictionary<string, string>()
                        , MessageType = Util.Util.GetTypeName(typeof(EndpointRegistrationRequest))
                        , Ordinal = 0
                        , PayLoad = payLoad
                        , Status = PersistedMessageStatusOptions.ReadyToProcess
                    };
                    msg.Routes.Add(new Route() { EndPointName = configure.myEndPoint.Name, EndPointType = configure.myEndPoint.EndPointType, Routed = DateTime.UtcNow });
                    IMessageRouter router = (IMessageRouter)configure.EndPoints[endPoint.EndPointName];
                    router.Publish(msg);
                }

                configure.EndPoints.ToList().ForEach(k => {

                    endPointsRunning.AddOrUpdate(k.Key, new pubStat() { Has = true, Processing = false }, (c, z) => { return new pubStat() { Has = true, Processing = false }; });

                });

                // TODO: Ping Endpoints
                //PingEndPoints();

                configure.Persitor.ToggleMarkAll(PersistedMessageStatusOptions.ReadyToProcess, PersistedMessageStatusOptions.Marked);

                restartTimer = new System.Timers.Timer();
                restartTimer.Interval = 1000;
                restartTimer.Enabled = true;
                restartTimer.AutoReset = false;
                restartTimer.Elapsed += Restart;
            } catch (Exception e) {
                throw new ServiceEndpointDownException("Bus Persistor is Down");

            }
        }

        private static void Restart(object sender, System.Timers.ElapsedEventArgs e) {
            StartProcessing();
        }

        private static void StartProcessing() {

            for (int i = 1; i <= maxThreads; i++) {

                Task t = taskThreads[i - 1];
                if (t == null || t.IsCompleted) {
                    //t = new Thread(PublishNext);

                    //capture i during the loop, since publishnext is running from the facotry, it may begin
                    //after i has been incremeneted.
                    int q = i;

                    t = Task.Run(() => { RouteNext(q); }, CTS.Token);

           
                    taskThreadsRunning.AddOrUpdate(i, true, (k, v) => { return true; });

                    taskThreads[i - 1] = t;
                }



            }

            //trigger threads to start waiting
            YieldNextThread();
            RaiseOnProcessing();

        }


        private static void StopProcessing(int threadId) {

            //if (!sourceCTS.IsCancellationRequested) { sourceCTS.Cancel(); }
            taskThreadsRunning.AddOrUpdate(threadId, false, (k, v) => {
                return false;
            });

            Stall(threadId);
        }

        private static void YieldNextThread() {
            resetEvent.Set();

        }


        public static string SendMessage<T>(T message) {



            if (stopped || stopping) {
                throw new BusNotRunningException("Bus has been stopped and cannot accept any new messages");
            }

            //serialize message
            string serialized = JsonConvert.SerializeObject(message, new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.All });


            //create peristed message
            PersistedMessage msg = new PersistedMessage(serialized) { DateStamp = DateTime.UtcNow };

            JObject asJO = JObject.Parse(serialized);
            msg.MessageType = asJO["$type"].ToString();


            //persist message to local storage
            try {

                int id = SyncGetPadding();

                var routes = configure.Routes.Where(g => g.TypeName.Equals(msg.MessageType, StringComparison.OrdinalIgnoreCase));
                List<PersistedMessage> messages = new List<PersistedMessage>();

                foreach (MessageTypeMapping m in routes) {

                    IEndPoint endPoint = configure.EndPoints.FirstOrDefault(g => g.Key == m.EndPointName).Value;

                    PersistedMessage copied = new PersistedMessage() {
                        PayLoad = msg.PayLoad
                        , Headers = msg.Headers
                        , MessageType = msg.MessageType
                        , Status = PersistedMessageStatusOptions.ReadyToProcess
                        , Ordinal = id
                        , DateStamp = DateTime.UtcNow
                        , TransactionID = msg.TransactionID
                        , Queue = m.EndPointName
                    };
                    copied.Routes.Add(new Route() {
                        EndPointName = m.EndPointName
                        , EndPointType = endPoint.EndPointType
                        , Routed = DateTime.UtcNow
                    });
                    messages.Add(copied);
                }

                if (configure.Persitor.ServiceIsDown()) {
                    throw new ServiceEndpointDownException("Message Peristor is Down");
                }

                Action whenDone = new Action(() => {
                    IEnumerable<MessageTypeMapping> pubs = routes;

                    foreach (MessageTypeMapping m in pubs) {
                        endPointsRunning.AddOrUpdate(m.EndPointName, new pubStat() { Has = true, Processing = false }, (k, v) => {

                            if (!v.Processing) {
                                return new pubStat() { Has = true, Processing = false };
                            } else {
                                return v;
                            }
                        });
                    }

                    StartProcessing();

                });

                TxRM rm = new TxRM();

                string transactionId = rm.SendMessage(messages, configure.Persitor, whenDone);


                return transactionId;


            } catch (Exception) {
                throw;
            }




        }

        public static void ReceiveMessage(PersistedMessage message) {

            if (stopped || stopping) {
                throw new BusNotRunningException("Bus has been stopped and cannot accept any new messages");
            }

            try {
                int id = SyncGetPadding();

                var routes = configure.Routes.Where(g => g.TypeName.Equals(message.MessageType, StringComparison.OrdinalIgnoreCase) && g.Direction == MessageDirectionOptions.Outbound);
                List<PersistedMessage> messages = new List<PersistedMessage>();

                List<string> endPointsToTrigger = new List<string>();

                //persist a copy of the message for each endpoint
                foreach (MessageTypeMapping m in routes) {

                    IEndPoint endPoint = configure.EndPoints.FirstOrDefault(g => g.Key == m.EndPointName).Value;
                    if (endPointsToTrigger.Count(g => g.Equals(m.EndPointName, StringComparison.OrdinalIgnoreCase)) == 0) {
                        endPointsToTrigger.Add(m.EndPointName);
                    }
                    PersistedMessage copied = (PersistedMessage)message.Clone();

                    copied.Queue = m.EndPointName;
                    copied.Ordinal = id;
                    copied.Status = PersistedMessageStatusOptions.ReadyToProcess;
                    copied.Routes.Add(new Route() {
                        EndPointType = endPoint.EndPointType
                        , EndPointName = m.EndPointName
                        , Routed = DateTime.UtcNow
                    });
                    messages.Add(copied);
                }

                //in other words, if nothing is listening, don't bother persisting.
                if (messages.Count > 0) {
                    if (configure.Persitor.ServiceIsDown()) {
                        throw new ServiceEndpointDownException("Message Peristor is Down");
                    }

                    configure.Persitor.Persist(messages);

                }

                foreach (string n in endPointsToTrigger) {
                    endPointsRunning.AddOrUpdate(n, new pubStat() { Has = true, Processing = false }, (k, v) => {

                        if (!v.Processing) {
                            return new pubStat() { Has = true, Processing = false };
                        } else {
                            return v;
                        }
                    });
                }


            } catch {
                throw;
            }

            StartProcessing();

        }

        // TODO: add sender to persistedmessages.route
        // TODO: need to tell process to grab reschedules (i.e., endpointswithmessage = true)
        private static EndPointResponse RouteMessage(PersistedMessage msg) {

            EndPointResponse toReturn = new EndPointResponse() { Status = false };
            try {

                MessageTypeMapping route = configure.Routes.FirstOrDefault(g => g.EndPointName.Equals(msg.Queue, StringComparison.OrdinalIgnoreCase) && g.TypeName.Equals(msg.MessageType, StringComparison.OrdinalIgnoreCase));
                IEndPoint endPoint = configure.EndPoints.FirstOrDefault(g => g.Key.Equals(msg.Queue, StringComparison.OrdinalIgnoreCase)).Value;
                if (typeof(IMessageHandler).IsAssignableFrom(endPoint.GetType())) {

                    IMessageHandler handler = (IMessageHandler)endPoint;
                    toReturn.EndPointType = EndPointTypeOptions.Handler;
                    try {
                        HandlerResponse response = handler.Handle(msg);
                        toReturn.Status = response.Status == HandlerStatusOptions.Handled;
                        toReturn.HandlerResponse = response;
                    } catch {
                        toReturn.HandlerResponse = HandlerResponse.Exception();
                    }

                } else {
                    IMessageRouter router = (IMessageRouter)endPoint;
                    toReturn.EndPointType = router.EndPointType;
                    if (!router.ServiceIsDown()) {

                        RouteResponse response = router.Publish(msg);
                        toReturn = new EndPointResponse() { EndPointType = router.EndPointType, Status = response.Status, RouteResponse = response };
                    } else {
                        
                        toReturn.Status = route.DiscardIfDown;
                    }
                }

            } catch (Exception) {
                
            }

            return toReturn;
        }

        private static void RouteNext(int threadId) {
            RaiseOnThreadStarted();

            //Console.WriteLine("Thread {0} Waiting", myThreadId);
            bool process = true;

            while (process && !CTS.IsCancellationRequested) {
                if (resetEvent.WaitOne()) {

                    //shut down loop unless we have a publisher to query
                    process = false;
                    if (!CTS.IsCancellationRequested) {


                        //Console.WriteLine("Thread {0} going", myThreadId);
                        int newThreadId = threadId + 1;
                        if (newThreadId == (maxThreads + 1)) { newThreadId = 1; }



                        string q = SyncGetNextEndPointWithMessages();
                        if (!string.IsNullOrEmpty(q)) {


                            process = true;
                            //get next message to publish
                            PersistedMessage msg = SyncGetMessageToProcess(q, configure.Persitor);

                            //if we found a message, reset so next thread can look
                            //otherwise, don't reset... this will effectively stop all threads until soemthing else
                            //notifies that there is something to process.
                            if (msg != null) {

                                configure.Persitor.Mark(msg.Id, PersistedMessageStatusOptions.Processing);
                                //we have our value, so pass on to next thread
                                YieldNextThread();



                                //publish message
                                EndPointResponse response = RouteMessage(msg);

                                if (!configure.Persitor.ServiceIsDown()) {
                                    try {
                                        if (response.Status) {
                                            //if localstorage experiences an error, it will shut itself down
                                            var popped = configure.Persitor.Pop(msg.Id);
                                        } else {
                                            if (response.EndPointType == EndPointTypeOptions.Handler) {
                                                if (response.HandlerResponse.Status == HandlerStatusOptions.Reschedule) {
                                                    var popped = configure.Persitor.Reschedule(msg.Id, response.HandlerResponse.RescheduleIncrement);

                                                } else {
                                                    var popped = configure.Persitor.Mark(msg.Id, PersistedMessageStatusOptions.Exception);
                                                }
                                            } else { //router
                                                //set it back to marked so it will try again
                                                var popped = configure.Persitor.Mark(msg.Id, PersistedMessageStatusOptions.Marked);
                                            }
                                        }

                                        endPointsRunning.AddOrUpdate(q, new pubStat() { Has = true, Processing = false }, (k, v) => {
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

                                endPointsRunning.AddOrUpdate(q, new pubStat() { Has = false, Processing = false }, (k, v) => {
                                    return new pubStat() { Has = false, Processing = false };
                                });

                                YieldNextThread();
                            }

                        } else {
                            //if we don't have a q to process, allow the next thread to do its 
                            //work so it can shut itself down, too
                            process = false;
                            YieldNextThread();
                        }


                        //} else {

                        //    process = true;
                        //    //err, not my turn, pass on to next.
                        //    sourceRE.Set();

                        //}
                    }
                }



            } // end while cancellation token
            StopProcessing(threadId);
        }

        private static string SyncGetNextEndPointWithMessages() {
            string toReturn = null;
            lock (lockObj) {

                
                int current = currentEndpoint;
                
                bool processEndPoint = true;

    
                // TODO: ?? should all endpoints be registered with lowercase b/c of indexor

                // TODO: add config for restart time span

                //if the subscription subscriber has messages, we need to process those immediately
                //this ensures that the subscriber gets all messsages from the time it subscribes
                string registrationQueue = Util.Util.GetTypeName(typeof(EndpointRegistrationRequest)).ToLower();
                pubStat s = endPointsRunning[registrationQueue];
                if (s.Has && !s.Processing) { // if message on queue, but not processing... process it immediately
                    toReturn = registrationQueue;
                    endPointsRunning.AddOrUpdate(registrationQueue, new pubStat() { Has = true, Processing = true }, (k, v) => {
                        return new pubStat() { Has = true, Processing = true };
                    });
                    processEndPoint = false;
                } else if (s.Has && s.Processing) { //if message on queue, but already processing, don't process anything else until queue is done

                    //if we are processing subscription requests, stop all processing until done.

                    toReturn = null;
                    processEndPoint = false;
                }



                if (processEndPoint) {
                    int next = current + 1;
                    //a thread is already using current, so loop through remaining and grab next
                    for (int i = 0; i < (endPointsRunning.Count); i++) {

                        if (next >= endPointsRunning.Count) { next = 0; }
                        string q = endPointsRunning.ToList()[next].Key;
                        pubStat stat = endPointsRunning[q];

                        IEndPoint endPoint = configure.EndPoints[q];
                        bool forceHandle = false;
                        if (endPoint.EndPointType == EndPointTypeOptions.Handler) {
                            IMessageHandler handler = (IMessageHandler)endPoint;
                            forceHandle = handler.Parallel;
                        }

                        if (stat.Has && (!stat.Processing || forceHandle)) {
                            toReturn = q;
                            endPointsRunning.AddOrUpdate(q, new pubStat() { Has = true, Processing = true }, (k, v) => {
                                return new pubStat() { Has = true, Processing = true };
                            });
                            currentEndpoint = next;
                            break;
                        }
                        next++;
                    }

                    // TODO : WTF am i doing this?
                    if (string.IsNullOrEmpty(toReturn)) {
                        currentEndpoint = next - 1;
                        if (currentEndpoint < 0) {
                            currentEndpoint = endPointsRunning.Count - 1;
                        }
                    }

                }



            }
            return toReturn;
        }


        //shared stuff

        private static PersistedMessage SyncGetMessageToProcess(string q, IPersist storage) {

            PersistedMessage toReturn = null;
            lock (lockObj) {

                if (!storage.ServiceIsDown()) {
                    //get the next one from the database
                    try {

                        if (stopped || stopping) {
                            toReturn = null;
                        } else {

                            toReturn = storage.PeekAndMarkNext(q, PersistedMessageStatusOptions.Marked);
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

        

    }
}
