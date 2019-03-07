package data

import (
	"database/sql"
	"fmt"
)

// Parameters of the subscription
type SubscriptionParams struct {
	SubscriptionId     int32
	Concurrency        uint32
	ConcurrencyOfRetry uint32
	IntervalOfSending  uint32
	// Process timeout in milliseconds
	// ProcessTimeout is significant. Many checks relies on it, 0 means it has not a customized params, all params are in default value.
	ProcessTimeout  uint32
	ReceptionUri    string
	AlerterEmails   string
	AlerterReceiver string
	// 是否接收压测环境消息.
	ReceiveBenchMsgs    bool
	AlerterPhoneNumbers string
	AlerterEnabled      bool

	// 报警间隔，单位: 秒，默认180秒
	AlarmInterval int64
	// 错误次数计数间隔，单位: 秒，默认180秒
	IntervalOfErrorMonitorAlert int64
	// 发送失败阈值: 0<n<10，默认7次
	MessageFailureAlertThreshold uint16
	// 失败次数，默认120
	SubscriptionTotalFailureAlertThreshold int32
	// 消息堆积的报警极限，默认5000
	MessageBlockedAlertThreshold int64
	// 消息超过阈值时丢弃
	DropMessageThreshold int
}

func GetSubscriptionParamsById(subscriptionId int32) (sub SubscriptionParams, err error) {
	var (
		db *DB
	)
	db, err = GetDb()
	if err != nil {
		return
	}
	sqlStr := fmt.Sprintf(`SELECT param_name FROM %s
	WHERE subscription_id=?
	`, DB_TABLE_SUBSCRIPTION_PARAMS)
	rows, err := db.Query(sqlStr, subscriptionId)
	if err != nil {
		return
	}
	defer rows.Close()
	sub.SubscriptionId = subscriptionId
	var paramNames []string
	var rowV *sql.Row
	for rows.Next() {
		var paramName string
		err = rows.Scan(&paramName)
		if err != nil {
			return
		}
		paramNames = append(paramNames, paramName)
	}
	var noParam interface{}
	for _, paramName := range paramNames {
		sqlStr := fmt.Sprintf(`SELECT param_value FROM %s
	WHERE subscription_id=? AND param_name=?
	`, DB_TABLE_SUBSCRIPTION_PARAMS)
		rowV = db.QueryRow(sqlStr, subscriptionId, paramName)
		switch paramName {
		case "Concurrency":
			err = rowV.Scan(&sub.Concurrency)
		case "ConcurrencyOfRetry":
			err = rowV.Scan(&sub.ConcurrencyOfRetry)
		case "IntervalOfSending":
			err = rowV.Scan(&sub.IntervalOfSending)
		case "ProcessTimeout":
			err = rowV.Scan(&sub.ProcessTimeout)
		case "ReceptionUri":
			err = rowV.Scan(&sub.ReceptionUri)
		case "AlerterEmails":
			err = rowV.Scan(&sub.AlerterEmails)
		case "AlerterPhoneNumbers":
			err = rowV.Scan(&sub.AlerterPhoneNumbers)
		case "AlerterReceiver":
			err = rowV.Scan(&sub.AlerterReceiver)
		case "ReceiveBenchMsgs":
			err = rowV.Scan(&sub.ReceiveBenchMsgs)
		case "AlerterEnabled":
			err = rowV.Scan(&sub.AlerterEnabled)
		case "IntervalOfErrorMonitorAlert":
			err = rowV.Scan(&sub.IntervalOfErrorMonitorAlert)
		case "MessageFailureAlertThreshold":
			err = rowV.Scan(&sub.MessageFailureAlertThreshold)
		case "SubscriptionTotalFailureAlertThreshold":
			err = rowV.Scan(&sub.SubscriptionTotalFailureAlertThreshold)
		case "MessageBlockedAlertThreshold":
			err = rowV.Scan(&sub.MessageBlockedAlertThreshold)
		case "AlarmInterval":
			err = rowV.Scan(&sub.AlarmInterval)
		case "DropMessageThreshold":
			err = rowV.Scan(&sub.DropMessageThreshold)
		default:
			// rowV必须被scan,避免row不能被关闭而占用连接。
			rowV.Scan(&noParam)
		}
		if err != nil {
			return
		}
	}
	return
}
