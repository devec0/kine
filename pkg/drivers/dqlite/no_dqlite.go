// +build !dqlite

package dqlite

import (
	"context"
	"errors"

	"github.com/devec0/kine/pkg/drivers/generic"
	"github.com/devec0/kine/pkg/server"
)

func New(ctx context.Context, datasourceName string, connPoolConfig generic.ConnectionPoolConfig) (server.Backend, error) {
	return nil, errors.New(`this binary is built without dqlite support, compile with "-tags dqlite"`)
}
