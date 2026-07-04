package cas

import (
	"context"
	"os"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

type hardlinkingFileFetcher struct {
	base           FileFetcher
	cacheDirectory filesystem.Directory
	maxFiles       int
	maxSize        int64
	useClonefile   bool

	filesLock      sync.RWMutex
	filesSize      map[string]int64
	filesTotalSize int64

	evictionLock sync.Mutex
	evictionSet  eviction.Set[string]

	downloadsLock sync.Mutex
	downloads     map[string]<-chan struct{}
}

// NewHardlinkingFileFetcher is an adapter for FileFetcher that stores
// files in an internal directory. After successfully downloading files
// at the target location, they are linked into the cache. Future calls
// for the same file link them from the cache to the target location.
// This reduces the amount of network traffic needed.
//
// When useClonefile is set, files are staged with clonefile(2) (APFS
// copy-on-write) instead of link(2). This is required on Darwin so that
// self-resolving executables (e.g. hermetic Python interpreters) see
// their input-root path rather than the shared cache-directory inode.
// See UseClonefile in the worker configuration for details.
func NewHardlinkingFileFetcher(base FileFetcher, cacheDirectory filesystem.Directory, maxFiles int, maxSize int64, useClonefile bool, evictionSet eviction.Set[string]) FileFetcher {
	return &hardlinkingFileFetcher{
		base:           base,
		cacheDirectory: cacheDirectory,
		maxFiles:       maxFiles,
		maxSize:        maxSize,
		useClonefile:   useClonefile,

		filesSize: map[string]int64{},

		evictionSet: evictionSet,

		downloads: map[string]<-chan struct{}{},
	}
}

// installFile stages a file from one directory into another, either by
// hardlinking (link(2)) or, when useClonefile is set, by APFS
// copy-on-write cloning (clonefile(2)).
func (ff *hardlinkingFileFetcher) installFile(fromDirectory filesystem.Directory, fromName path.Component, toDirectory filesystem.Directory, toName path.Component) error {
	if ff.useClonefile {
		return fromDirectory.Clonefile(fromName, toDirectory, toName)
	}
	return fromDirectory.Link(fromName, toDirectory, toName)
}

func (ff *hardlinkingFileFetcher) makeSpace(size int64) error {
	for len(ff.filesSize) > 0 && (len(ff.filesSize) >= ff.maxFiles || ff.filesTotalSize+size > ff.maxSize) {
		// Remove a file from disk.
		key := ff.evictionSet.Peek()
		if err := ff.cacheDirectory.Remove(path.MustNewComponent(key)); err != nil && !os.IsNotExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to remove cached file %#v", key)
		}

		// Remove file from bookkeeping.
		ff.evictionSet.Remove()
		ff.filesTotalSize -= ff.filesSize[key]
		delete(ff.filesSize, key)
	}
	return nil
}

func (ff *hardlinkingFileFetcher) GetFile(ctx context.Context, blobDigest digest.Digest, directory filesystem.Directory, name path.Component, isExecutable bool) error {
	key := blobDigest.GetKey(digest.KeyWithoutInstance)
	if isExecutable {
		key += "+x"
	} else {
		key += "-x"
	}

	for {
		// If the file is present in the cache, stage it to the destination.
		if err := ff.tryLinkFromCache(key, directory, name); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}

		// A download is required. Let's see if one is already in progress.
		ff.downloadsLock.Lock()
		wait, ok := ff.downloads[key]
		if ok {
			// A download is already in progress. Wait for it to finish.
			ff.downloadsLock.Unlock()
			select {
			case <-wait:
				// Download finished. Loop back to try linking from
				// cache. If missing (download failed or other issue),
				// we'll attempt a new download.
				continue
			case <-ctx.Done():
				return util.StatusFromContext(ctx)
			}
		}

		// Start a new download.
		break
	}
	newWait := make(chan struct{})
	ff.downloads[key] = newWait
	ff.downloadsLock.Unlock()

	defer func() {
		ff.downloadsLock.Lock()
		delete(ff.downloads, key)
		ff.downloadsLock.Unlock()
		close(newWait)
	}()

	// Check cache again in case another download completed between our initial
	// tryLinkFromCache() call and acquiring the download lock.
	if err := ff.tryLinkFromCache(key, directory, name); err == nil || !os.IsNotExist(err) {
		return err
	}

	// Download the file at the intended location.
	if err := ff.base.GetFile(ctx, blobDigest, directory, name, isExecutable); err != nil {
		return err
	}

	ff.filesLock.Lock()
	defer ff.filesLock.Unlock()
	if _, ok := ff.filesSize[key]; !ok {
		ff.evictionLock.Lock()
		defer ff.evictionLock.Unlock()

		// Remove old files from the cache if necessary.
		sizeBytes := blobDigest.GetSizeBytes()
		if err := ff.makeSpace(sizeBytes); err != nil {
			return err
		}

		// Stage the file into the cache.
		if err := ff.installFile(directory, name, ff.cacheDirectory, path.MustNewComponent(key)); err != nil && !os.IsExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to add cached file %#v", key)
		}
		ff.evictionSet.Insert(key)
		ff.filesSize[key] = sizeBytes
		ff.filesTotalSize += sizeBytes
	} else {
		// Even though the file is part of our bookkeeping, we
		// observed it didn't exist. Repair this inconsistency.
		if err := ff.installFile(directory, name, ff.cacheDirectory, path.MustNewComponent(key)); err != nil && !os.IsExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to repair cached file %#v", key)
		}
	}
	return nil
}

// tryLinkFromCache attempts to stage a file from the cache into the
// build directory (via link(2) or clonefile(2); see installFile). It
// returns os.ErrNotExist if the file is not in the cache bookkeeping, or
// if it was in bookkeeping but missing on disk.
func (ff *hardlinkingFileFetcher) tryLinkFromCache(key string, directory filesystem.Directory, name path.Component) error {
	ff.filesLock.RLock()
	defer ff.filesLock.RUnlock()

	if _, ok := ff.filesSize[key]; ok {
		ff.evictionLock.Lock()
		ff.evictionSet.Touch(key)
		ff.evictionLock.Unlock()

		if err := ff.installFile(ff.cacheDirectory, path.MustNewComponent(key), directory, name); err == nil {
			// Successfully staged the file to its destination.
			return nil
		} else if !os.IsNotExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to stage cached file %#v", key)
		}
	}
	return os.ErrNotExist
}
