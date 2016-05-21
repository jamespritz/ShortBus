using ShortBus;
using ShortBus.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;

namespace Publisher.Controllers
{
    public class MessageController : Controller
    {
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        [HttpPost]
        public JsonResult Publish(PersistedMessage message) {


            ShortBus.Bus.BroadcastMessage(message);
            

            return Json(new { result = true }, JsonRequestBehavior.AllowGet);


            //return result:true

        }

        [HttpGet]
        public JsonResult HelloWorld() {
            Assembly assembly = Assembly.GetExecutingAssembly();
            

            var attribute = (GuidAttribute)assembly.GetCustomAttributes(typeof(GuidAttribute), true)[0];
            var id = attribute.Value;

          

            return Json(new { id = ShortBus.Util.Util.GetApplicationGuid().ToString()
                , name = ShortBus.Util.Util.GetApplicationName() }, JsonRequestBehavior.AllowGet);
        }

    }
}