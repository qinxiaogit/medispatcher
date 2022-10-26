package sender

import (
	"github.com/qinxiaogit/medispatcher/config"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMain(t *testing.T) {
	Convey("Main", t, func() {
		Convey("config setup", func() {
			e := config.Setup()
			So(e, ShouldBeNil)
		})
	})

}

func TestNewSubscriptionParams(t *testing.T) {
	Convey("NewSubscriptionParams", t, func() {
		Convey("Store test", func() {
			sub := NewSubscriptionParams()

			So(sub.IntervalOfErrorMonitorAlert, ShouldEqual, INTERVAL_OF_ERROR_MONITOR_ALERT)
			So(sub.MessageFailureAlertThreshold, ShouldEqual, MESSAGE_FAILURE_ALERT_THRESHOLD)
			So(sub.SubscriptionTotalFailureAlertThreshold, ShouldEqual, SUBSCRIPTION_TOTAL_FAILURE_ALERT_THRESHOLD)
			So(sub.MessageBlockedAlertThreshold, ShouldEqual, MESSAGE_BLOCKED_ALERT_THRESHOLD)

			sub.SubscriptionId = 1001
			sub.Concurrency = 100
			sub.ConcurrencyOfRetry = 101
			sub.IntervalOfSending = 102
			sub.ProcessTimeout = 103
			sub.ReceptionUri = "xxx"
			sub.AlerterEmails = "x@x.com"
			sub.AlerterPhoneNumbers = "10086"
			sub.AlerterEnabled = true

			sub.IntervalOfErrorMonitorAlert = 6
			sub.MessageFailureAlertThreshold = 7
			sub.SubscriptionTotalFailureAlertThreshold = 8
			sub.MessageBlockedAlertThreshold = 9
			e := sub.Store(sub.SubscriptionId)
			So(e, ShouldBeNil)
		})

		Convey("Load test", func() {

			sub := NewSubscriptionParams()
			sub.SubscriptionId = 1001
			sub.Load(sub.SubscriptionId)

			So(sub.SubscriptionId, ShouldEqual, 1001)
			So(sub.Concurrency, ShouldEqual, 100)
			So(sub.ConcurrencyOfRetry, ShouldEqual, 101)
			So(sub.IntervalOfSending, ShouldEqual, 102)
			So(sub.ProcessTimeout, ShouldEqual, 103)
			So(sub.ReceptionUri, ShouldEqual, "xxx")
			So(sub.AlerterEmails, ShouldEqual, "x@x.com")
			So(sub.AlerterPhoneNumbers, ShouldEqual, "10086")
			So(sub.AlerterEnabled, ShouldBeTrue)

			So(sub.IntervalOfErrorMonitorAlert, ShouldEqual, 6)
			So(sub.MessageFailureAlertThreshold, ShouldEqual, 7)
			So(sub.SubscriptionTotalFailureAlertThreshold, ShouldEqual, 8)
			So(sub.MessageBlockedAlertThreshold, ShouldEqual, 9)
		})

		Convey("Store&Load test default", func() {
			sub := NewSubscriptionParams()

			So(sub.IntervalOfErrorMonitorAlert, ShouldEqual, INTERVAL_OF_ERROR_MONITOR_ALERT)
			So(sub.MessageFailureAlertThreshold, ShouldEqual, MESSAGE_FAILURE_ALERT_THRESHOLD)
			So(sub.SubscriptionTotalFailureAlertThreshold, ShouldEqual, SUBSCRIPTION_TOTAL_FAILURE_ALERT_THRESHOLD)
			So(sub.MessageBlockedAlertThreshold, ShouldEqual, MESSAGE_BLOCKED_ALERT_THRESHOLD)

			sub.SubscriptionId = 1002
			sub.Concurrency = 100
			sub.ConcurrencyOfRetry = 101
			sub.IntervalOfSending = 102
			sub.ProcessTimeout = 103
			sub.ReceptionUri = "xxx"
			sub.AlerterEmails = "x@x.com"
			sub.AlerterPhoneNumbers = "10086"
			sub.AlerterEnabled = true

			sub.IntervalOfErrorMonitorAlert = 0
			sub.MessageFailureAlertThreshold = 0
			sub.SubscriptionTotalFailureAlertThreshold = 0
			sub.MessageBlockedAlertThreshold = 0
			e := sub.Store(sub.SubscriptionId)
			So(e, ShouldBeNil)

			sub.Load(sub.SubscriptionId)
			So(sub.SubscriptionId, ShouldEqual, 1002)
			So(sub.Concurrency, ShouldEqual, 100)
			So(sub.ConcurrencyOfRetry, ShouldEqual, 101)
			So(sub.IntervalOfSending, ShouldEqual, 102)
			So(sub.ProcessTimeout, ShouldEqual, 103)
			So(sub.ReceptionUri, ShouldEqual, "xxx")
			So(sub.AlerterEmails, ShouldEqual, "x@x.com")
			So(sub.AlerterPhoneNumbers, ShouldEqual, "10086")
			So(sub.AlerterEnabled, ShouldBeTrue)

			So(sub.IntervalOfErrorMonitorAlert, ShouldEqual, INTERVAL_OF_ERROR_MONITOR_ALERT)
			So(sub.MessageFailureAlertThreshold, ShouldEqual, MESSAGE_FAILURE_ALERT_THRESHOLD)
			So(sub.SubscriptionTotalFailureAlertThreshold, ShouldEqual, SUBSCRIPTION_TOTAL_FAILURE_ALERT_THRESHOLD)
			So(sub.MessageBlockedAlertThreshold, ShouldEqual, MESSAGE_BLOCKED_ALERT_THRESHOLD)
		})
	})
}
