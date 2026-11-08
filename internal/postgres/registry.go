package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sudopower/olivaw-fleet/internal/registry"
)

// RegistryStore implements registry.Store.
type RegistryStore struct{ db *DB }

func (db *DB) Registry() *RegistryStore { return &RegistryStore{db: db} }

func (s *RegistryStore) ListRobots(ctx context.Context) ([]registry.Robot, error) {
	rows, err := s.db.pool.Query(ctx,
		`SELECT id, site_id, manufacturer, serial_number, name, model FROM robots`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (registry.Robot, error) {
		var r registry.Robot
		err := row.Scan(&r.ID, &r.SiteID, &r.Device.Manufacturer, &r.Device.Serial, &r.Name, &r.Model)
		return r, err
	})
}

func (s *RegistryStore) CreateSite(ctx context.Context, site registry.Site) error {
	_, err := s.db.pool.Exec(ctx, `INSERT INTO sites (id, name) VALUES ($1, $2)`, site.ID, site.Name)
	return mapErr(err, "site "+string(site.ID))
}

func (s *RegistryStore) CreateRobot(ctx context.Context, r registry.Robot) error {
	_, err := s.db.pool.Exec(ctx, `
		INSERT INTO robots (id, site_id, manufacturer, serial_number, name, model)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		r.ID, r.SiteID, r.Device.Manufacturer, r.Device.Serial, r.Name, r.Model)
	return mapErr(err, fmt.Sprintf("robot %s/%s", r.Device.Manufacturer, r.Device.Serial))
}

// mapErr turns constraint violations into registry errors.
func mapErr(err error, what string) error {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "23505": // unique_violation
			return fmt.Errorf("%s: %w", what, registry.ErrConflict)
		case "23503": // foreign_key_violation
			return fmt.Errorf("%s: referenced %w", what, registry.ErrNotFound)
		}
	}
	return err
}
