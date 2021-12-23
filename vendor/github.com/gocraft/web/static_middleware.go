package web

import (
	"net/http"
	"path/filepath"
	"strings"
)

// StaticOption configures how StaticMiddlewareDir handles url paths and index files for directories.
// If set, Prefix is removed from the start of the url path before attempting to serve a directory or file.
// If set, IndexFile is the index file to serve when the url path maps to a directory.
type StaticOption struct {
	Prefix    string
	IndexFile string
}

// StaticMiddleware is the same as StaticMiddlewareFromDir, but accepts a
// path string for backwards compatibility.
func StaticMiddleware(path string, option ...StaticOption) func(ResponseWriter, *Request, NextMiddlewareFunc) {
	return StaticMiddlewareFromDir(http.Dir(path), option...)
}

// StaticMiddlewareFromDir returns a middleware that serves static files from the specified http.FileSystem.
// This middleware is great for development because each file is read from disk each time and no
// special caching or cache headers are sent.
//
// If a path is requested which maps to a folder with an index.html folder on your filesystem,
// then that index.html file will be served.
func StaticMiddlewareFromDir(dir http.FileSystem, options ...StaticOption) func(ResponseWriter, *Request, NextMiddlewareFunc) {
	var option StaticOption
	if len(options) > 0 {
		option = options[0]
	}
	return func(w ResponseWriter, req *Request, next NextMiddlewareFunc) {
		if req.Method != "GET" && req.Method != "HEAD" {
			next(w, req)
			return
		}

		file := req.URL.Path
		if option.Prefix != "" {
			if !strings.HasPrefix(file, option.Prefix) {
				next(w, req)
				return
			}
			file = file[len(option.Prefix):]
		}

		f, err := dir.Open(file)
		if err != nil {
			next(w, req)
			return
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			next(w, req)
			return
		}

		// If the file is a directory, try to serve an index file.
		// If no index is available, DO NOT serve the directory to avoid
		// Content-Length issues. Simply skip to the next middleware, and return
		// a 404 if no route with the same name is handled. 
		if fi.IsDir() {
			if option.IndexFile != "" {
				file = filepath.Join(file, option.IndexFile)
				f, err = dir.Open(file)
				if err != nil {
					next(w, req)
					return
				}
				defer f.Close()

				fi, err = f.Stat()
				if err != nil || fi.IsDir() {
					next(w, req)
					return
				}
			} else {
				next(w, req)
				return
			}
		}
		http.ServeContent(w, req.Request, file, fi.ModTime(), f)
	}
}
