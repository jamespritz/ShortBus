using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Default {
    public class DefaultSourceSettings {

        /*
         * As a source, i need to know
         *  where to persist messages in queue
         *  where the publishers are (name, type, settings)
         *  what the messages are, and where to publish them (type, publisher name)
         *  
         */

        public String MongoConnectionString { get; set; }
       
        public RESTSettings PublisherSettings { get;set; }

    }

    /*
     * As a subscriber i need to know
     *  where to persist messages in queue
     *  where the publishers are so I can subscribe (name, address, settings)
     *  what messages i am subscribing to, and from which publisher i should expect them
     *      -- type, publisher name, settings
     */
    public class DefaultSubscriberSettings {
        public String MongoConnectionString { get; set; }
        public RESTSettings Endpoint { get; set; }
        public RESTSettings Publisher { get; set; }
    }


    /*
     * as a publisher i need to know
     * where to persist messages in queue
     * where to put my subscriptions
     */
    public class DefaultPublisherSettings {
        public String MongoConnectionString { get; set; }

    }

    public class DefaultDistributorSettings {

    }

}
