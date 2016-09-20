using ShortBus.Publish;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShortBus.Persistence;
using System.Net;
using Newtonsoft.Json;
using System.IO;
using ShortBus.Routing;

namespace ShortBus.Default {


    public class RESTSettings {

        public RESTSettings(string url, EndPointTypeOptions endPointType) {
            this.URL = url;
            this.EndPointType = endPointType;
        }

        public string URL { get;set; }
        public EndPointTypeOptions EndPointType { get; set; }
    }

    public class RESTEndPoint : IMessageRouter {

        private bool ImDown = false;
        private RESTSettings settings = null;

        EndPointTypeOptions IEndPoint.EndPointType {
            get {
                return settings.EndPointType;
            }
        }

        public RESTEndPoint(RESTSettings settings) {
            this.settings = settings;
        }


        RouteResponse IMessageRouter.Publish(PersistedMessage message) {

            return this.Post(message, "PostMessage");

        }



        bool IMessageRouter.ResetConnection() {
            this.ImDown = false;
            return !this.ImDown;
        }
        bool IMessageRouter.ResetConnection(string endPointAddress) {
            this.settings.URL = endPointAddress;
            return ((IMessageRouter)this).ResetConnection();
        }

        bool IMessageRouter.ServiceIsDown() {
            return this.ImDown;
        }



        //postmessage
        private RouteResponse Post(PersistedMessage post, string command) {

            RouteResponse toReturn = new RouteResponse() { PayLoad = null, Status = false };
            try {
                string url = settings.URL + string.Format(@"/api/message/{0}", command);

                url = url.ToLower().Replace("localhost", Util.Util.GetLocalIP());

                HttpWebRequest req = (HttpWebRequest)HttpWebRequest.Create(new Uri(url));
                //req.ContentType = @"application/x-www-form-urlencoded";
                req.ContentType = "application/json";
                req.Method = "POST";

                string payload = JsonConvert.SerializeObject(post);

                //string postData = string.Format(@"&message={0}", payload);
                string postData = string.Format(@"{0}", payload);
                req.ContentLength = postData.Length;


                req.Timeout = 5000;
                ServicePointManager.ServerCertificateValidationCallback = delegate { return true; };
                ServicePointManager.DnsRefreshTimeout = 5000;

                Task<Stream> reqTask = req.GetRequestStreamAsync();
                reqTask.Wait();

                using (Stream reqStream = req.GetRequestStream())
                using (StreamWriter sw = new StreamWriter(reqStream, System.Text.Encoding.ASCII)) {
                    sw.Write(postData);

                    sw.Flush();
                    sw.Close();
                    reqStream.Close();

                }


                string responseText = null;
                //Task<WebResponse> resTask = req.GetResponseAsync();
                //resTask.Wait();
                // WebResponse response = req.GetResponse();
                //using (WebResponse response = resTask.Result)
                using (WebResponse response = req.GetResponse())
                using (Stream resStream = response.GetResponseStream())
                using (StreamReader rdr = new StreamReader(resStream)) {
                    responseText = rdr.ReadToEnd();



                    rdr.Close();
                    response.Close();
                }

                toReturn = JsonConvert.DeserializeObject<RouteResponse>(responseText);


            } catch (System.AggregateException ae) {

                this.ImDown = true;
            
            } catch (WebException e) {
           
                this.ImDown = true;
                
            } catch (Exception e) {
                this.ImDown = true;
            }

            return toReturn;
        }

    }



}
