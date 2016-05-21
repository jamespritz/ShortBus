using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShortBus.Subscriber {

    public enum HandlerStatusOptions {
        Handled = 1,
        Exception = 2,
        Reschedule = 3
    }
    public class HandlerResponse {

        static HandlerResponse() { }
        private HandlerResponse() { }

        public static HandlerResponse Handled() {
            return new HandlerResponse() { rescheduleIncrement = TimeSpan.MinValue, status = HandlerStatusOptions.Handled };
        }
        public static HandlerResponse Exception() {
            return new HandlerResponse() { rescheduleIncrement = TimeSpan.MinValue, status = HandlerStatusOptions.Exception };
        }
        public static HandlerResponse Retry(TimeSpan span) {
            return new HandlerResponse() { rescheduleIncrement = span, status = HandlerStatusOptions.Reschedule };
        }

        private HandlerStatusOptions status;
        private TimeSpan rescheduleIncrement;

        public HandlerStatusOptions Status { get { return this.status; } }
        public TimeSpan RescheduleIncrement {  get { return this.rescheduleIncrement; } }

    }
}
