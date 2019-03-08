package filelog

import (
	"encoding/json"
	"fmt"
	"medispatcher/config"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultMaxFileSize = 1 << 30
)

var (
	fileMapLock sync.Mutex
	fileMap     = make(map[int32]*Filelog)
)

func Write(id int32, v interface{}) error {
	fileMapLock.Lock()
	l, ok := fileMap[id]
	if !ok {
		l = New(config.GetConfig().DropMessageLogDir, fmt.Sprintf("subscription_%d", id))
		fileMap[id] = l
	}
	fileMapLock.Unlock()
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = l.Write(append(data, '\n'))
	return err
}

type Filelog struct {
	Dir         string
	Name        string
	MaxFileSize int64
	fd          *os.File
	position    int64
	mu          sync.Mutex
}

func New(logdir string, name string) *Filelog {
	return &Filelog{
		Dir:         logdir,
		Name:        name,
		MaxFileSize: defaultMaxFileSize,
	}
}

func (l *Filelog) Write(b []byte) (int, error) {
	err := l.rotate()
	if err != nil {
		return 0, err
	}

	n, err := l.fd.Write(b)
	if err != nil {
		return 0, err
	}
	l.position += int64(n)
	return n, nil
}

func (l *Filelog) rotate() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fd == nil || l.position > l.MaxFileSize {
		filename := l.getFilename()
		fpath := filepath.Join(l.Dir, filename)
		fd, err := os.OpenFile(fpath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}
		if l.fd != nil {
			l.fd.Close()
		}
		l.fd = fd
		l.position = 0
	}
	return nil
}

func (l *Filelog) getFilename() string {
	t := time.Now().Unix()
	return fmt.Sprintf("%s_%d.data", l.Name, t)
}

func (l *Filelog) Close() error {
	return l.fd.Close()
}
