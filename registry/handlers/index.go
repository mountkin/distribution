package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/docker/distribution/registry/index"
	"github.com/gorilla/handlers"
)

// indexDispatcher constructs ihe index handler api endpoint.
func indexDispatcher(ctx *Context, r *http.Request) http.Handler {
	indexHandler := &indexHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		"GET":     http.HandlerFunc(indexHandler.GetPage),
		"OPTIONS": http.HandlerFunc(indexHandler.AnnounceCsphere),
	}
}

// indexHandler handles requests for lists of index under a repository name.
type indexHandler struct {
	*Context
}

// GetTags returns a json list of index for a specific image name.
func (ih *indexHandler) GetPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	r.ParseForm()
	skip, _ := strconv.Atoi(r.Form.Get("skip"))
	limit, _ := strconv.Atoi(r.Form.Get("limit"))
	keyword := r.Form.Get("keyword")
	queryArgs := index.QueryArgs{
		Keyword: keyword,
		Skip:    skip,
		Limit:   limit,
	}

	page, err := ih.index.GetPage(queryArgs)
	if err == nil {
		enc := json.NewEncoder(w)
		err = enc.Encode(page)
	}

	if err != nil {
		ih.Errors = append(ih.Errors, err)
		return
	}
}

func (ih *indexHandler) AnnounceCsphere(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("cSphere", "true")
	w.WriteHeader(200)
}
