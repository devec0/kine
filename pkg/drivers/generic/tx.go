package generic

import (
	"context"
	"database/sql"

	log "k8s.io/klog/v2"
)

type Tx struct {
	x *sql.Tx
	d *Generic
}

func (d *Generic) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	log.Infof("TX BEGIN")
	x, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{
		x: x,
		d: d,
	}, nil
}

func (t *Tx) Commit() error {
	log.Infof("TX COMMIT")
	return t.x.Commit()
}

func (t *Tx) MustCommit() {
	if err := t.Commit(); err != nil {
		log.Fatalf("Transaction commit failed: %v", err)
	}
}

func (t *Tx) Rollback() error {
	log.Infof("TX ROLLBACK")
	return t.x.Rollback()
}

func (t *Tx) MustRollback() {
	if err := t.Rollback(); err != nil {
		if err != sql.ErrTxDone {
			log.Fatalf("Transaction rollback failed: %v", err)
		}
	}
}

func (t *Tx) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := t.queryRow(ctx, compactRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (t *Tx) SetCompactRevision(ctx context.Context, revision int64) error {
	log.Infof("TX SETCOMPACTREVISION %v", revision)
	_, err := t.execute(ctx, t.d.UpdateCompactSQL, revision)
	return err
}

func (t *Tx) Compact(ctx context.Context, revision int64) (int64, error) {
	log.Infof("TX COMPACT %v", revision)
	res, err := t.execute(ctx, t.d.CompactSQL, revision, revision)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (t *Tx) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return t.query(ctx, t.d.GetRevisionSQL, revision)
}

func (t *Tx) DeleteRevision(ctx context.Context, revision int64) error {
	log.Infof("TX DELETEREVISION %v", revision)
	_, err := t.execute(ctx, t.d.DeleteSQL, revision)
	return err
}

func (t *Tx) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := t.queryRow(ctx, revSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (t *Tx) query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	log.Infof("TX QUERY %v : %s", args, Stripped(sql))
	return t.x.QueryContext(ctx, sql, args...)
}

func (t *Tx) queryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	log.Infof("TX QUERY ROW %v : %s", args, Stripped(sql))
	return t.x.QueryRowContext(ctx, sql, args...)
}

func (t *Tx) execute(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	log.Infof("TX EXEC %v : %s", args, Stripped(sql))
	return t.x.ExecContext(ctx, sql, args...)
}
