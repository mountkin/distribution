package storage

import (
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/manifest"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

type layerStore struct {
	repository *repository
	tomb       tombstone
}

func (ls *layerStore) Exists(digest digest.Digest) (bool, error) {
	ctx := ls.repository.ctx
	context.GetLogger(ctx).Debug("(*layerStore).Exists")

	tombstoneExists, err := ls.tomb.tombstoneExists(ctx, ls.repository.Name(), digest)
	if err != nil {
		return false, err
	}
	if tombstoneExists {
		return false, nil
	}

	bp, err := ls.path(digest)
	if err != nil {
		switch err.(type) {
		case distribution.ErrUnknownLayer:
			return false, nil
		}

		return false, err
	}

	_, err = ls.repository.driver.Stat(ctx, bp)
	if err != nil {
		return false, err
	}

	if err != nil {
		switch err.(type) {
		case distribution.ErrUnknownLayer:
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (ls *layerStore) Fetch(dgst digest.Digest) (distribution.Layer, error) {
	ctx := ls.repository.ctx
	context.GetLogger(ctx).Debug("(*layerStore).Fetch")

	exists, err := ls.Exists(dgst)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, distribution.ErrUnknownLayer{FSLayer: manifest.FSLayer{BlobSum: dgst}}
	}

	bp, err := ls.path(dgst)
	if err != nil {
		return nil, err
	}

	fr, err := newFileReader(ctx, ls.repository.driver, bp)
	if err != nil {
		return nil, err
	}

	return &layerReader{
		fileReader: *fr,
		digest:     dgst,
	}, nil
}

// Upload begins a layer upload, returning a handle. If the layer upload
// is already in progress or the layer has already been uploaded, this
// will return an error.
func (ls *layerStore) Upload() (distribution.LayerUpload, error) {
	ctx := ls.repository.ctx
	context.GetLogger(ctx).Debug("(*layerStore).Upload")

	// NOTE(stevvooe): Consider the issues with allowing concurrent upload of
	// the same two layers. Should it be disallowed? For now, we allow both
	// parties to proceed and the the first one uploads the layer.

	uuid := uuid.New()
	startedAt := time.Now().UTC()

	path, err := ls.repository.pm.path(uploadDataPathSpec{
		name: ls.repository.Name(),
		uuid: uuid,
	})

	if err != nil {
		return nil, err
	}

	startedAtPath, err := ls.repository.pm.path(uploadStartedAtPathSpec{
		name: ls.repository.Name(),
		uuid: uuid,
	})

	if err != nil {
		return nil, err
	}

	// Write a startedat file for this upload
	if err := ls.repository.driver.PutContent(ctx, startedAtPath, []byte(startedAt.Format(time.RFC3339))); err != nil {
		return nil, err
	}

	return ls.newLayerUpload(uuid, path, startedAt)
}

// Resume continues an in progress layer upload, returning the current
// state of the upload.
func (ls *layerStore) Resume(uuid string) (distribution.LayerUpload, error) {
	ctx := ls.repository.ctx
	context.GetLogger(ctx).Debug("(*layerStore).Resume")

	startedAtPath, err := ls.repository.pm.path(uploadStartedAtPathSpec{
		name: ls.repository.Name(),
		uuid: uuid,
	})

	if err != nil {
		return nil, err
	}

	startedAtBytes, err := ls.repository.driver.GetContent(ctx, startedAtPath)
	if err != nil {
		switch err := err.(type) {
		case storagedriver.PathNotFoundError:
			return nil, distribution.ErrLayerUploadUnknown
		default:
			return nil, err
		}
	}

	startedAt, err := time.Parse(time.RFC3339, string(startedAtBytes))
	if err != nil {
		return nil, err
	}

	path, err := ls.repository.pm.path(uploadDataPathSpec{
		name: ls.repository.Name(),
		uuid: uuid,
	})

	if err != nil {
		return nil, err
	}

	return ls.newLayerUpload(uuid, path, startedAt)
}

// Delete deletes a blob referenced by digest from storage by creating a tombstone
// file.  Subsequent layer operations must check the presence of the tombstone
func (ls *layerStore) Delete(dgst digest.Digest) error {
	ctx := ls.repository.ctx
	context.GetLogger(ctx).Debug("(*layerStore).Delete")

	tombstoneExists, err := ls.tomb.tombstoneExists(ctx, ls.repository.Name(), dgst)
	if err != nil {
		return err
	}
	if tombstoneExists {
		return distribution.ErrUnknownLayer{
			FSLayer: manifest.FSLayer{BlobSum: dgst},
		}
	}

	if err := ls.tomb.putTombstone(ctx, ls.repository.Name(), dgst); err != nil {
		return err
	}

	return nil
}

// newLayerUpload allocates a new upload controller with the given state.
func (ls *layerStore) newLayerUpload(uuid, path string, startedAt time.Time) (distribution.LayerUpload, error) {
	fw, err := newFileWriter(ls.repository.ctx, ls.repository.driver, path)
	if err != nil {
		return nil, err
	}

	lw := &layerWriter{
		layerStore:         ls,
		uuid:               uuid,
		startedAt:          startedAt,
		bufferedFileWriter: *fw,
		tomb: tombstone{
			pm:     defaultPathMapper,
			driver: ls.repository.driver,
		},
	}

	lw.setupResumableDigester()

	return lw, nil
}

func (ls *layerStore) path(dgst digest.Digest) (string, error) {
	// We must traverse this path through the link to enforce ownership.
	layerLinkPath, err := ls.repository.pm.path(layerLinkPathSpec{name: ls.repository.Name(), digest: dgst})
	if err != nil {
		return "", err
	}

	blobPath, err := ls.repository.blobStore.resolve(layerLinkPath)

	if err != nil {
		switch err := err.(type) {
		case storagedriver.PathNotFoundError:
			return "", distribution.ErrUnknownLayer{
				FSLayer: manifest.FSLayer{BlobSum: dgst},
			}
		default:
			return "", err
		}
	}

	return blobPath, nil
}
