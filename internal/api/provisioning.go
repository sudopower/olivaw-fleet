package api

import (
	"net/http"

	"github.com/sudopower/olivaw-fleet/internal/fleet"
	"github.com/sudopower/olivaw-fleet/internal/registry"
	"github.com/sudopower/olivaw-fleet/internal/telemetry"
)

type siteRequest struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (s *server) createSite(w http.ResponseWriter, r *http.Request) {
	var req siteRequest
	if !decode(w, r, &req) {
		return
	}
	site := registry.Site{ID: fleet.SiteID(req.ID), Name: req.Name}
	if err := s.Registry.CreateSite(r.Context(), site); err != nil {
		s.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, req)
}

type robotRequest struct {
	SiteID       string `json:"site_id"`
	Manufacturer string `json:"manufacturer"`
	Serial       string `json:"serial"`
	Name         string `json:"name,omitempty"`
	Model        string `json:"model,omitempty"`
}

type robotResponse struct {
	ID string `json:"id"`
	robotRequest
}

func (s *server) provisionRobot(w http.ResponseWriter, r *http.Request) {
	var req robotRequest
	if !decode(w, r, &req) {
		return
	}
	rb, err := s.Registry.Provision(r.Context(), registry.Robot{
		SiteID: fleet.SiteID(req.SiteID),
		Device: telemetry.Device{Manufacturer: req.Manufacturer, Serial: req.Serial},
		Name:   req.Name,
		Model:  req.Model,
	})
	if err != nil {
		s.writeDomainError(w, err)
		return
	}
	req.Name = rb.Name
	writeJSON(w, http.StatusCreated, robotResponse{ID: string(rb.ID), robotRequest: req})
}
