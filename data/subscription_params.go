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
	ProcessTimeout      uint32
	ReceptionUri        string
	AlerterEmails       string
	AlerterPhoneNumbers string
	AlerterEnabled      bool
}

func GetSubscriptionParamsById(subscriptionId int32) (sub SubscriptionParams, err error) {
	var (
		db  *DB
	)
	db, err = GetDb()
	if err != nil {
		return
	}
	sqlStr :=  fmt.Sprintf(`SELECT param_name FROM %s
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
	for _, paramName := range paramNames{
		sqlStr :=  fmt.Sprintf(`SELECT param_value FROM %s
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
		case "AlerterEnabled":
			err = rowV.Scan(&sub.AlerterEnabled)
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
