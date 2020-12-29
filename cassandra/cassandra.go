package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/shyyawn/go-to/config"
	log "github.com/shyyawn/go-to/logging"
	"sync"
	"time"
)

type Cassandra struct {
	cluster *gocql.ClusterConfig
	session *gocql.Session
	lock    *sync.RWMutex
}

func (gCass *Cassandra) Init() error {
	gCass.lock = &sync.RWMutex{}
	_ = gCass.Cluster()
	return nil
}

func (gCass *Cassandra) Session() *gocql.Session {
	if gCass.session == nil || gCass.session.Closed() {
		if gCass.cluster == nil {
			_ = gCass.Cluster()
		}
		gCass.lock.Lock()
		defer gCass.lock.Unlock()
		var err error
		gCass.session, err = gCass.cluster.CreateSession()
		if err != nil {
			log.Error("Unable to connect to cassandra", err)
		}
	}
	return gCass.session
}

func (gCass *Cassandra) Cluster() *gocql.ClusterConfig {
	gCass.lock.Lock()
	defer gCass.lock.Unlock()
	gCass.cluster = gocql.NewCluster(config.AppConfig.GetStringSlice("cassandra.hosts")...)

	gCass.cluster.Port = config.AppConfig.GetInt("cassandra.port")
	gCass.cluster.Keyspace = config.AppConfig.GetString("cassandra.keyspace")
	gCass.cluster.Timeout = time.Second * time.Duration(config.AppConfig.GetInt64("cassandra.timeout"))
	gCass.cluster.ReconnectInterval = time.Second * time.Duration(config.AppConfig.GetInt64("cassandra.reconnect_interval"))
	gCass.cluster.Consistency = gocql.LocalQuorum
	return gCass.cluster
}
