package index

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/notifications"
	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultLimit = 20
)

type Repository struct {
	Repository string `json:"repository"`
	Tags       []Tag  `json:"tags"`
}

type Tag struct {
	Repository  string    `json:"repository"`
	Tag         string    `json:"tag"`
	Digest      string    `json:"digest"`
	Url         string    `json:"url"`
	Status      string    `json:"status"`
	Description string    `json:"description"`
	TargetURL   string    `json:"target_url"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type QueryArgs struct {
	Keyword string
	Skip    int
	Limit   int
}

func (self *QueryArgs) prepare() {
	if self.Skip < 0 {
		self.Skip = 0
	}
	if self.Limit < 1 {
		self.Limit = defaultLimit
	}
}

type IndexService struct {
	db *sql.DB
}

func New(configuration *configuration.Configuration) (*IndexService, error) {
	var (
		err   error
		srv   = &IndexService{}
		stmts [4]string
	)
	storageParams := configuration.Storage.Parameters()
	dbPath := filepath.Join(fmt.Sprint(storageParams["rootdirectory"]), "registry.sqlite3")
	os.MkdirAll(filepath.Dir(dbPath), 0755)
	srv.db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		logrus.Error("Failed to open database: ", err)
		return nil, err
	}

	stmts[0] = `create table if not exists tags(
		id         integer primary key,
		repository varchar(256),
		digest     varchar(80),
		url        varchar(256),
		tag        varchar(256),
		status     varchar(32),
		description varchar(256),
		target_url varchar(256),
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`
	stmts[1] = `create unique index if not exists idx_name_tag on tags(repository, tag)`
	stmts[2] = `create table if not exists repositories(
		id         integer primary key,
		repository varchar(256)
	)`
	stmts[3] = `create unique index if not exists idx_name on repositories(repository)`
	for _, stmt := range stmts {
		if _, err := srv.db.Exec(stmt); err != nil {
			logrus.Error("Failed to prepare database: ", err)
			return nil, err
		}
	}

	return srv, nil
}

func (self *IndexService) Write(events ...notifications.Event) error {
	for _, event := range events {
		if event.Target.MediaType == schema1.ManifestMediaType {
			if event.Action == notifications.EventActionDelete {
				if err := self.delete(event); err != nil {
					return err
				}
			} else if event.Action == notifications.EventActionPush {
				// if err := self.add(event); err != nil {
				// 	return err
				// }
			}
		}
	}
	return nil
}

func (self *IndexService) delete(event notifications.Event) error {
	tag := self.parseTag(event.Target.URL)
	query := "delete from tags where repository=? and tag=?"
	_, err := self.db.Exec(query, event.Target.Repository, tag)
	if err == nil {
		_, err = self.db.Exec("delete from repositories where repository not in (select distinct repository from tags)")
	}
	return err
}

func (self *IndexService) add(event notifications.Event) error {
	target := event.Target
	query := "replace into repositories(repository) values(?)"

	if _, err := self.db.Exec(query, target.Repository); err != nil {
		logrus.Error("sqlite insert: ", err)
		return err
	}
	query = "replace into tags(repository, tag, digest, url, updated_at, status, description, target_url) values(?,?,?,?,?,'unset','','')"
	tag := self.parseTag(event.Target.URL)
	if _, err := self.db.Exec(query, target.Repository, tag, string(target.Digest), target.URL, time.Now()); err != nil {
		logrus.Error("sqlite insert: ", err)
		return err
	}
	return nil
}

func (self *IndexService) parseTag(url string) string {
	parts := strings.Split(url, "/")
	if l := len(parts); l > 1 {
		return parts[l-1]
	}
	return ""
}

func (self *IndexService) Close() error {
	logrus.Debug("index service close")
	self.db.Close()
	return nil
}

func (self *IndexService) Sink() notifications.Sink {
	return self
}

func (self *IndexService) GetPage(args QueryArgs) ([]Repository, error) {
	args.prepare()
	query := "select repository from repositories "
	if len(args.Keyword) > 0 {
		query += " where repository like ? "
	}
	query += " limit ? offset ?"

	stmt, err := self.db.Prepare(query)
	if err != nil {
		logrus.Error("select prepare: ", err)
		return nil, err
	}
	defer stmt.Close()

	var rows *sql.Rows

	if len(args.Keyword) > 0 {
		rows, err = stmt.Query("%"+args.Keyword+"%", args.Limit, args.Skip)
	} else {
		rows, err = stmt.Query(args.Limit, args.Skip)
	}

	if err != nil {
		logrus.Error("sqlite query: ", err)
		return nil, err
	}

	records := []Repository{}
	for rows.Next() {
		record := Repository{Tags: []Tag{}}
		err = rows.Scan(&record.Repository)
		if err == nil {
			var tags *sql.Rows
			tags, err = self.db.Query("select repository, tag, digest, url, status, description, target_url, updated_at from tags where repository = ?", record.Repository)
			if err == nil {
				for tags.Next() {
					tag := Tag{}
					err = tags.Scan(&tag.Repository, &tag.Tag, &tag.Digest, &tag.Url, &tag.Status, &tag.Description, &tag.TargetURL, &tag.UpdatedAt)
					if err == nil {
						record.Tags = append(record.Tags, tag)
					}
				}
			}
		}
		if err != nil {
			logrus.Error("failed to scan rows: ", err)
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

func (self *IndexService) SetTagStatus(repo, tag, status, description, target_url string) error {
	_, err := self.db.Exec("update tags set status=?, description=?, target_url=? where repository=? and tag=?", status, description, target_url, repo, tag)
	return err
}
