package endpoint

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/devec0/kine/pkg/drivers/dqlite"
	"github.com/devec0/kine/pkg/drivers/generic"
	"github.com/devec0/kine/pkg/drivers/mysql"
	"github.com/devec0/kine/pkg/drivers/pgsql"
	"github.com/devec0/kine/pkg/drivers/sqlite"
	"github.com/devec0/kine/pkg/server"
	"github.com/devec0/kine/pkg/tls"
        log "k8s.io/klog/v2"	
	"google.golang.org/grpc"
)

const (
	KineSocket      = "unix://kine.sock"
	SQLiteBackend   = "sqlite"
	DQLiteBackend   = "dqlite"
	ETCDBackend     = "etcd3"
	MySQLBackend    = "mysql"
	PostgresBackend = "postgres"
)

type Config struct {
	GRPCServer           *grpc.Server
	Listener             string
	Endpoint             string
	ConnectionPoolConfig generic.ConnectionPoolConfig

	tls.Config
}

type ETCDConfig struct {
	Endpoints   []string
	TLSConfig   tls.Config
	LeaderElect bool
}

func Listen(ctx context.Context, config Config) (ETCDConfig, error) {
	log.Infof("Kine listener is configuring and starting")
	log.Infof("Kine storage endpoint: %s", config.Endpoint)
	driver, dsn := ParseStorageEndpoint(config.Endpoint)
	if driver == ETCDBackend {
		return ETCDConfig{
			Endpoints:   strings.Split(config.Endpoint, ","),
			TLSConfig:   config.Config,
			LeaderElect: true,
		}, nil
	}

	leaderelect, backend, err := getKineStorageBackend(ctx, driver, dsn, config)
	if err != nil {
		log.Infof("Kine storage backend failed to configure")
		return ETCDConfig{}, errors.Wrap(err, "building kine")
	}

	if err := backend.Start(ctx); err != nil {
		log.Infof("Kine backend failed to start")
		return ETCDConfig{}, errors.Wrap(err, "starting kine backend")
	}

	listen := config.Listener
	if listen == "" {
		listen = KineSocket
	}

	log.Infof("Creating kine gRPC server")
	b := server.New(backend)
	grpcServer := grpcServer(config)
	b.Register(grpcServer)

	log.Infof("Kine gRPC listener starting")
	listener, err := createListener(listen)
	if err != nil {
		log.Errorf("Failed to create kind gRPC listener: %v", err)
		return ETCDConfig{}, err
	}

	go func() {
		log.Infof("Kine gRPC server is starting")
		if err := grpcServer.Serve(listener); err != nil {
			log.Errorf("Kine server shutdown: %v", err)
		}
		<-ctx.Done()
		grpcServer.Stop()
		listener.Close()
	}()

	return ETCDConfig{
		LeaderElect: leaderelect,
		Endpoints:   []string{listen},
		TLSConfig:   tls.Config{},
	}, nil
}

func createListener(listen string) (ret net.Listener, rerr error) {
	network, address := networkAndAddress(listen)

	if network == "unix" {
		if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
			log.Warningf("failed to remove socket %s: %v", address, err)
		}
		defer func() {
			if err := os.Chmod(address, 0600); err != nil {
				rerr = err
			}
		}()
	}

	log.Infof("Kine listening on %s://%s", network, address)
	return net.Listen(network, address)
}

func grpcServer(config Config) *grpc.Server {
	log.Infof("Creating kine gRPC server")
	if config.GRPCServer != nil {
		return config.GRPCServer
	}
	return grpc.NewServer()
}

func getKineStorageBackend(ctx context.Context, driver, dsn string, cfg Config) (bool, server.Backend, error) {
	var (
		backend     server.Backend
		leaderElect = true
		err         error
	)
	switch driver {
	case SQLiteBackend:
		leaderElect = false
		backend, err = sqlite.New(ctx, dsn, cfg.ConnectionPoolConfig)
	case DQLiteBackend:
		backend, err = dqlite.New(ctx, dsn, cfg.ConnectionPoolConfig)
	case PostgresBackend:
		backend, err = pgsql.New(ctx, dsn, cfg.Config, cfg.ConnectionPoolConfig)
	case MySQLBackend:
		backend, err = mysql.New(ctx, dsn, cfg.Config, cfg.ConnectionPoolConfig)
	default:
		return false, nil, fmt.Errorf("storage backend is not defined")
	}

	return leaderElect, backend, err
}

func ParseStorageEndpoint(storageEndpoint string) (string, string) {
	log.Infof("Parsing storage endpoint address for kvsql/kine: %s", storageEndpoint)
	network, address := networkAndAddress(storageEndpoint)
	switch network {
	case "":
		return SQLiteBackend, ""
	case "http":
		fallthrough
	case "https":
		return ETCDBackend, address
	}
	return network, address
}

func networkAndAddress(str string) (string, string) {
	log.Infof("Parsing listen address for kvsql/kine: %s", str)
	parts := strings.SplitN(str, "://", 2)
	if len(parts) > 1 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}
