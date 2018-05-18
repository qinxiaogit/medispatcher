package data

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/vmihailenco/msgpack.v2"
	"medispatcher/config"
	"strings"
	"time"
	"sync"
	"strconv"
)

var dbCreateLock = new(sync.Mutex)
var db *DB
type DB struct {
	*sql.DB
	dsn                   string
}

// TODO: full implementation of serialization.
type RedisConn struct {
	conn redis.Conn
	id   int64
}

var redisPoolIdle = map[int64]*RedisConn{}
var redisPoolUsing = map[int64]*RedisConn{}
var redisPoolAccessLock = make(chan int32, 1)

func (myconn *RedisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	var isRead, isWrite bool

	if strings.Index(REDIS_WRITE_COMMANDS, strings.ToUpper(commandName)) != -1 {
		isWrite = true
	} else if strings.Index(REDIS_READ_COMMANDS, strings.ToUpper(commandName)) != -1 {
		isRead = true
	}
	if isWrite {
		var temp []byte
		// args[0] is the key name.
		for i, _ := range args[1:] {
			i += 1
			temp, err = myconn.serialize(args[i])
			if err != nil {
				return
			}
			args[i] = temp
		}
	}
	reply, err = myconn.conn.Do(commandName, args...)
	if err != nil {
		return
	}

	if isRead {
		switch reply.(type) {
		case []byte:
			reply, err = myconn.deserialize(reply.([]byte))
		case []interface{}:
			temp := reply.([]interface{})
			// for command LPOP,BLPOP,RPOP,BRPOP etc.
			temp[1], err = myconn.deserialize(temp[1].([]byte))
			if err != nil {
				reply = nil
				return
			}
			reply = temp
		}
	}
	return

}

func (_ *RedisConn) serialize(data interface{}) ([]byte, error) {
	b, e := msgpack.Marshal(data)
	if e != nil {
		return nil, errors.New(fmt.Sprintf("Redis failed to serialize data: %v", e))
	}
	return b, nil
}

func (_ *RedisConn) deserialize(data []byte) (i interface{},err  error) {
	err = msgpack.Unmarshal(data, &i)
	if err != nil {
		err = errors.New(fmt.Sprintf("Redis failed to deserialize data: %v", err))
	}
	return
}

func (myconn *RedisConn) Close() error {
	return myconn.conn.Close()
}
func (redis *RedisConn) Release() {
	getRedisPoolLock()
	defer releaseRedisPoolLock()
	delete(redisPoolUsing, redis.id)
	redisPoolIdle[redis.id] = redis
}

func (db *DB) Query(sqlStr string, args ...interface{}) (*sql.Rows, error) {
	return db.DB.Query(sqlStr, args...)
}

func (db *DB) QueryRow(sqlStr string, args ...interface{}) (*sql.Row) {
	return db.DB.QueryRow(sqlStr, args...)
}

func (db *DB) Insert(table string, data map[string]interface{}) (re sql.Result, err error) {
	fields := make([]string, len(data))
	fieldValues := make([]interface{}, len(fields))
	placeholder := make([]string, len(fieldValues))
	index := 0
	for field, d := range data {
		fields[index] = field
		fieldValues[index] = d
		placeholder[index] = "?"
		index++
	}

	sqlStr := `
		INSERT INTO %s (%s)
		VALUES (%s)
		`
	sqlStr = fmt.Sprintf(sqlStr, table, strings.Join(fields, ","), strings.Join(placeholder, ","))
	re, err = db.Exec(sqlStr, fieldValues...)
	return
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.DB.Exec(query, args...)
}


func getRedisPoolLock() bool {
	redisPoolAccessLock <- int32(1)
	return true
}

func releaseRedisPoolLock() bool {
	<-redisPoolAccessLock
	return true
}

func GetRedis() (*RedisConn, error) {
	redis := getRedisFromPool()
	if redis != nil {
		return redis, nil
	}

	if len(redisPoolUsing) > MAX_DB_CONNECTIONS {
		retryTimes := 0
		for {
			time.Sleep(time.Millisecond * 10)
			retryTimes += 1
			if retryTimes > MAX_DB_CONNECTIONS {
				break
			} else {
				redis = getRedisFromPool()
				if redis != nil {
					return redis, nil
				}
			}
		}
		return nil, errors.New(fmt.Sprintf("Max connections of pool reached: %v, try again later.", MAX_DB_CONNECTIONS))
	}
	redisNew, err := createRedisConn()
	redis = getRedisFromPool()
	if redis != nil {
		if err == nil {
			redisNew.Close()
		}
		return redis, nil
	} else {
		if err != nil {
			return nil, err
		}
		getRedisPoolLock()
		defer releaseRedisPoolLock()
		redisPoolUsing[redisNew.id] = redisNew
		redis = redisNew
	}
	return redis, err
}

func createRedisConn() (*RedisConn, error) {
	redisConfig := config.GetConfig().Redis
	redis, err := redis.DialTimeout("tcp", redisConfig["Addr"].(string), time.Second*1, time.Second*5, time.Second*5)
	if err != nil {
		return nil, err
	}
	redis.Do("SELECT", redisConfig["DbIndex"])
	id := time.Now().UnixNano()
	return &RedisConn{redis, id}, err
}

func GetDb() (*DB, error) {
	dbCreateLock.Lock()
	defer dbCreateLock.Unlock()
	if db != nil {
		return db, nil
	}
	var err error
	dbConfig := config.GetConfig().Database
	dsn := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?%v", dbConfig["User"], dbConfig["Password"], dbConfig["Host"], dbConfig["Port"], dbConfig["DbName"], dbConfig["Options"])
	dbC, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	maxConnI, err := strconv.ParseInt(fmt.Sprintf("%v", dbConfig["MaxConn"]), 10, 32)
	if err != nil{
		maxConnI = int64(MAX_DB_CONNECTIONS)
	} else if maxConnI > int64(MAX_DB_CONNECTIONS) {
		maxConnI = int64(MAX_DB_CONNECTIONS)
	}
	dbC.SetMaxOpenConns(int(maxConnI))
	db = &DB{
		DB: dbC,
		dsn: dsn,
	}
	return db, nil
}

func getRedisFromPool() *RedisConn {
	getRedisPoolLock()
	defer releaseRedisPoolLock()
	if len(redisPoolIdle) > 0 {
		for _, db := range redisPoolIdle {
			return db
		}
	}
	return nil
}
