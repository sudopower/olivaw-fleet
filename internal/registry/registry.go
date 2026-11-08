// Package registry knows which robots exist, which site each belongs to, and
// how each identifies itself on the wire.
//
// The ingest path calls Resolve for every message, so the registry keeps an
// in-memory index and only goes to storage when robots are provisioned or the
// index is refreshed.
package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/telemetry"
)

// Site is a location robots are deployed at.
type Site struct {
	ID   fleet.SiteID
	Name string
}

// Robot is a provisioned robot.
type Robot struct {
	ID     fleet.RobotID
	SiteID fleet.SiteID
	Device telemetry.Device
	Name   string
	Model  string
}

var (
	ErrInvalid  = errors.New("invalid")
	ErrConflict = errors.New("already exists")
	ErrNotFound = errors.New("not found")
)

// Store persists the registry. Implementations return errors wrapping
// ErrConflict and ErrNotFound where they apply.
type Store interface {
	ListRobots(ctx context.Context) ([]Robot, error)
	CreateSite(ctx context.Context, s Site) error
	CreateRobot(ctx context.Context, r Robot) error
}

// Registry is the in-memory index over the store.
type Registry struct {
	store Store

	mu       sync.RWMutex
	byDevice map[telemetry.Device]Robot
}

func New(store Store) *Registry {
	return &Registry{store: store, byDevice: make(map[telemetry.Device]Robot)}
}

// Load replaces the index with the store's contents. Call at startup, and
// periodically when more than one instance can provision robots.
func (r *Registry) Load(ctx context.Context) error {
	robots, err := r.store.ListRobots(ctx)
	if err != nil {
		return fmt.Errorf("load registry: %w", err)
	}
	idx := make(map[telemetry.Device]Robot, len(robots))
	for _, rb := range robots {
		idx[rb.Device] = rb
	}
	r.mu.Lock()
	r.byDevice = idx
	r.mu.Unlock()
	return nil
}

// Resolve maps a wire identity to a provisioned robot.
func (r *Registry) Resolve(d telemetry.Device) (Robot, bool) {
	r.mu.RLock()
	rb, ok := r.byDevice[d]
	r.mu.RUnlock()
	return rb, ok
}

// Len is the number of provisioned robots.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byDevice)
}

// CreateSite adds a site.
func (r *Registry) CreateSite(ctx context.Context, s Site) error {
	if s.ID == "" || s.Name == "" {
		return fmt.Errorf("%w: site needs id and name", ErrInvalid)
	}
	return r.store.CreateSite(ctx, s)
}

// Provision registers a robot so its telemetry is accepted. The platform
// assigns the robot ID; the device identity must be unique.
func (r *Registry) Provision(ctx context.Context, rb Robot) (Robot, error) {
	if rb.SiteID == "" || rb.Device.Manufacturer == "" || rb.Device.Serial == "" {
		return Robot{}, fmt.Errorf("%w: robot needs site, manufacturer and serial", ErrInvalid)
	}
	if rb.Name == "" {
		rb.Name = rb.Device.Serial
	}
	rb.ID = newRobotID()
	if err := r.store.CreateRobot(ctx, rb); err != nil {
		return Robot{}, err
	}
	r.mu.Lock()
	r.byDevice[rb.Device] = rb
	r.mu.Unlock()
	return rb, nil
}

func newRobotID() fleet.RobotID {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails
	return fleet.RobotID("rbt_" + hex.EncodeToString(b[:]))
}
