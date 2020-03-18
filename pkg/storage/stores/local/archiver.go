package local

import (
	"context"
	"flag"
	"fmt"
	"github.com/cortexproject/cortex/pkg/chunk"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/azure"

	"github.com/cortexproject/cortex/pkg/chunk/local"

	"github.com/cortexproject/cortex/pkg/chunk/aws"
	"github.com/cortexproject/cortex/pkg/chunk/gcp"

	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

const (
	// UpdateTypeFileDownloaded is for file downloaded update
	UpdateTypeFileDownloaded = iota
	// UpdateTypeFileRemoved is for file removed update
	UpdateTypeFileRemoved
	// UpdateTypeTableRemoved is for table removed update i.e all the files for a periodic table were removed
	UpdateTypeTableRemoved

	// ArchiverModeReadWrite is to allow both read and write
	ArchiverModeReadWrite = iota
	// ArchiverModeReadOnly is to allow only read operations
	ArchiverModeReadOnly
	// ArchiverModeWriteOnly is to allow only write operations
	ArchiverModeWriteOnly

	updateChanCapacity = 50
	// ArchiverFileUploadInterval defines interval at which we keep pushing updated or new files
	ArchiverFileUploadInterval = 15 * time.Minute
)

type StoreConfig struct {
	Store            string            `yaml:"store"`
	AWSStorageConfig aws.StorageConfig `yaml:"aws"`
	GCSConfig        gcp.GCSConfig     `yaml:"gcs"`
	FSConfig         local.FSConfig    `yaml:"filesystem"`
	Azure            azure.BlobStorageConfig `yaml:"azure"`
}

// ArchiverConfig is for configuring an archiver
type ArchiverConfig struct {
	Enable         bool          `yaml:"enable"`
	CacheLocation  string        `yaml:"cache_location"`
	CacheTTL       time.Duration `yaml:"cache_ttl"`
	StoreConfig    StoreConfig   `yaml:"store_config"`
	ResyncInterval time.Duration `yaml:"resync_interval"`
	IngesterName   string        `yaml:"-"`
	Mode           int           `yaml:"-"`
}

// ArchiveStoreClient define all the methods that a store needs to implement for managing objects
type ArchiveStoreClient interface {
	GetObject(ctx context.Context, objectName string) (io.ReadCloser, error)
	PutObject(ctx context.Context, objectName string, object io.ReadSeeker) error
	List(ctx context.Context, prefix string) (map[string]time.Time, error)
}

// RegisterFlags registers flags.
func (cfg *ArchiverConfig) RegisterFlags(f *flag.FlagSet) {
	storeFlagsPrefix := "boltdb.archiver."
	cfg.StoreConfig.AWSStorageConfig.RegisterFlagsWithPrefix(storeFlagsPrefix, f)
	cfg.StoreConfig.GCSConfig.RegisterFlagsWithPrefix(storeFlagsPrefix, f)
	cfg.StoreConfig.FSConfig.RegisterFlagsWithPrefix(storeFlagsPrefix, f)

	f.BoolVar(&cfg.Enable, "boltdb.archiver.enable", false, "Enable archival of boltdb files to a store")
	f.StringVar(&cfg.StoreConfig.Store, "boltdb.archiver.store", "filesystem", "Store for keeping boltdb files")
	f.StringVar(&cfg.CacheLocation, "boltdb.archiver.cache-location", "", "Cache location for restoring boltDB files for queries")
	f.DurationVar(&cfg.CacheTTL, "boltdb.archiver.cache-ttl", 24*time.Hour, "TTL for boltDB files restored in cache for queries")
	f.DurationVar(&cfg.ResyncInterval, "boltdb.archiver.resync-interval", 5*time.Minute, "Resync downloaded files with the store")
}

// Update holds a single update to a file or folder
type Update struct {
	UpdateType int
	TableName  string
	FilePath   string
}

// downloadedTable represents multiple boltdb files for same period uploaded by different ingesters
type downloadedTable struct {
	files        map[string]time.Time
	downloadedAt time.Time
}

// Archiver holds its configuration and progress of objects in sync
// Uploads boltdb files to storage with structure <boltdb-filename>/<ingester-name>
// Keeps syncing locally changed file to the storage and downloading latest changes from storage
// Cleans up downloaded files as per configured TTL
type Archiver struct {
	cfg             ArchiverConfig
	boltDbDirectory string
	objectClient    chunk.ObjectClient

	downloadedTables    map[string]*downloadedTable
	downloadedTablesMtx sync.RWMutex

	updatesChan           chan Update
	uploadedFilesMtime    map[string]time.Time
	uploadedFilesMtimeMtx sync.RWMutex
	done                  chan struct{}

	// We would use ingester name and startup timestamp for naming files while uploading so that
	// ingester does not override old files when using same id
	ingesterNameAndStartUpTs string
}

type file struct {
	name  string
	mtime time.Time
	path  string
}

// NewArchiver creates an archiver for syncing local objects with a store
func NewArchiver(cfg ArchiverConfig, boltDbDirectory string, archiveStoreClient chunk.ObjectClient) (*Archiver, error) {
	if err := chunk_util.EnsureDirectory(cfg.CacheLocation); err != nil {
		return nil, err
	}

	archiver := Archiver{
		cfg:                      cfg,
		boltDbDirectory:          boltDbDirectory,
		downloadedTables:         map[string]*downloadedTable{},
		updatesChan:              make(chan Update, updateChanCapacity),
		uploadedFilesMtime:       map[string]time.Time{},
		done:                     make(chan struct{}),
		ingesterNameAndStartUpTs: fmt.Sprintf("%s-%d", cfg.IngesterName, time.Now().Unix()),
	}

	archiver.objectClient = archiveStoreClient

	go archiver.loop()

	return &archiver, nil
}

func (a *Archiver) loop() {
	resyncTicker := time.NewTicker(a.cfg.ResyncInterval)
	defer resyncTicker.Stop()

	archiveFileTicker := time.NewTicker(ArchiverFileUploadInterval)
	defer archiveFileTicker.Stop()

	cacheTTLTicker := time.NewTicker(a.cfg.CacheTTL)
	defer cacheTTLTicker.Stop()

	for {
		select {
		case <-resyncTicker.C:
			err := a.syncAllTables(context.Background())
			if err != nil {
				level.Error(util.Logger).Log("msg", "error syncing archived boltdb files with store", "err", err)
			}
		case <-archiveFileTicker.C:
			err := a.archiveFiles(context.Background())
			if err != nil {
				level.Error(util.Logger).Log("msg", "error pushing archivable files to store", "err", err)
			}
		case <-cacheTTLTicker.C:
			err := a.removeExpiredTables()
			if err != nil {
				level.Error(util.Logger).Log("msg", "error cleaning up expired tables", "err", err)
			}
		case <-a.done:
			return
		}
	}
}

// Stop the archiver and push all the local files to the store
func (a *Archiver) Stop() {
	close(a.done)

	// Push all boltdb files to archive before returning
	err := a.archiveFiles(context.Background())
	if err != nil {
		level.Error(util.Logger).Log("msg", "error pushing archivable files to store", "err", err)
	}
}

func (a *Archiver) archiveFiles(ctx context.Context) error {
	if a.cfg.Mode == ArchiverModeReadOnly {
		return nil
	}

	files, err := a.listFiles(a.boltDbDirectory)
	if err != nil {
		return err
	}

	a.uploadedFilesMtimeMtx.Lock()
	defer a.uploadedFilesMtimeMtx.Unlock()

	for _, file := range files {
		// Checking whether file is updated after last push, if not skipping it
		uploadedFileMtime, isOK := a.uploadedFilesMtime[file.path]
		if isOK && !uploadedFileMtime.Before(file.mtime) {
			continue
		}

		f, err := os.Open(file.path)
		if err != nil {
			return err
		}

		// Files are stored with <periodic-table-name>/<ingester-name>
		objectName := fmt.Sprintf("%s/%s", file.name, a.ingesterNameAndStartUpTs)
		err = a.objectClient.PutObject(ctx, objectName, f)
		f.Close()
		if err != nil {
			return err
		}
		a.uploadedFilesMtime[file.path] = file.mtime
	}

	return nil
}

func (a *Archiver) listFiles(path string) ([]file, error) {
	filesInfo, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	files := []file{}
	for _, fileInfo := range filesInfo {
		if fileInfo.IsDir() {
			continue
		}
		files = append(files, file{fileInfo.Name(), fileInfo.ModTime(), filepath.Join(path, fileInfo.Name())})
	}

	return files, nil
}

// Sync i.e download all files for a table and keep pulling new and updated files
func (a *Archiver) Sync(ctx context.Context, name string) ([]string, error) {
	if a.cfg.Mode == ArchiverModeWriteOnly {
		return nil, nil
	}

	a.downloadedTablesMtx.RLock()

	downloadedTable, isOk := a.downloadedTables[name]
	if !isOk {
		a.downloadedTablesMtx.RUnlock()
		err := a.syncTable(ctx, name, false)
		if err != nil {
			return nil, err
		}

		a.downloadedTablesMtx.RLock()
		downloadedTable = a.downloadedTables[name]
	}

	indexFiles := make([]string, 0, len(downloadedTable.files))
	for downloadedFile := range downloadedTable.files {
		indexFiles = append(indexFiles, path.Join(a.cfg.CacheLocation, name, downloadedFile))
	}
	a.downloadedTablesMtx.RUnlock()
	return indexFiles, nil
}

func (a *Archiver) syncAllTables(ctx context.Context) error {
	if a.cfg.Mode == ArchiverModeWriteOnly {
		return nil
	}

	tablesToSync := a.listDownloadedTables()

	for _, tableToSync := range tablesToSync {
		err := a.syncTable(ctx, tableToSync, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Archiver) syncTable(ctx context.Context, tableName string, sendUpdates bool) error {
	// listing tables from store
	objectNamesWithMtime, err := a.objectClient.List(ctx, tableName)
	if err != nil {
		return err
	}

	indexDir := path.Join(a.cfg.CacheLocation, tableName)
	err = chunk_util.EnsureDirectory(indexDir)
	if err != nil {
		return err
	}

	a.downloadedTablesMtx.RLock()
	_, isOk := a.downloadedTables[tableName]
	a.downloadedTablesMtx.RUnlock()

	if !isOk {
		a.downloadedTablesMtx.Lock()
		a.downloadedTables[tableName] = &downloadedTable{
			files:        map[string]time.Time{},
			downloadedAt: time.Now(),
		}
		a.downloadedTablesMtx.Unlock()
	}

	fileNames := make(map[string]struct{}, len(objectNamesWithMtime))

	// downloading files, skipping which are already downloaded and upto date
	for _, object := range objectNamesWithMtime {
		filename := strings.Split(object.Key, "/")[1]
		fileNames[filename] = struct{}{}
		filePath := path.Join(indexDir, filename)

		a.downloadedTablesMtx.RLock()
		isUptoDate := false

		// Checking whether file was updated in the store after we downloaded it, if not skipping download
		downloadedFileMtime, isOk := a.downloadedTables[tableName].files[filename]
		if isOk && downloadedFileMtime == object.ModifiedAt {
			isUptoDate = true
		}

		a.downloadedTablesMtx.RUnlock()
		if isUptoDate {
			continue
		}

		readCloser, err := a.objectClient.GetObject(ctx, object.Key)
		if err != nil {
			return err
		}

		defer readCloser.Close()

		f, err := os.Create(filePath)
		if err != nil {
			return err
		}

		_, err = io.Copy(f, readCloser)
		if err != nil {
			return err
		}

		if sendUpdates {
			a.updatesChan <- Update{UpdateType: UpdateTypeFileDownloaded, TableName: tableName, FilePath: filePath}
		}

		a.downloadedTablesMtx.Lock()
		a.downloadedTables[tableName].files[filename] = object.ModifiedAt
		a.downloadedTablesMtx.Unlock()
	}

	if sendUpdates {
		// Clean up removed files from the store.
		// We want to clean up removed files only when we are sending updates for taking appropriate action when files are removed
		// If we are not cleaning up now, it should get handled when re-sync kicks in which sends the updates
		a.downloadedTablesMtx.Lock()
		defer a.downloadedTablesMtx.Unlock()

		downloadedTable := a.downloadedTables[tableName]
		for fileName := range downloadedTable.files {
			if _, isOK := fileNames[fileName]; !isOK {
				filePath := path.Join(indexDir, fileName)
				err := os.Remove(filePath)
				if err != nil {
					return err
				}
				a.updatesChan <- Update{UpdateType: UpdateTypeFileRemoved, TableName: tableName, FilePath: filePath}
			}
		}
	}

	return nil
}

func (a *Archiver) listDownloadedTables() []string {
	a.downloadedTablesMtx.RLock()
	defer a.downloadedTablesMtx.RUnlock()

	tablesInSync := make([]string, len(a.downloadedTables))
	i := 0

	for tableName := range a.downloadedTables {
		tablesInSync[i] = tableName
		i++
	}

	return tablesInSync
}

func (a *Archiver) removeExpiredTables() error {
	expiry := time.Now().Add(-a.cfg.CacheTTL)
	tablesToRemove := []string{}

	a.downloadedTablesMtx.RLock()

	for tableName, downloadedTable := range a.downloadedTables {
		if downloadedTable.downloadedAt.Before(expiry) {
			tablesToRemove = append(tablesToRemove, tableName)
		}
	}

	a.downloadedTablesMtx.RUnlock()

	for _, tableToRemove := range tablesToRemove {
		err := a.removeTable(tableToRemove)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Archiver) removeTable(tableName string) error {
	a.downloadedTablesMtx.Lock()
	defer a.downloadedTablesMtx.Unlock()

	a.updatesChan <- Update{UpdateType: UpdateTypeTableRemoved, TableName: tableName}

	delete(a.downloadedTables, tableName)
	return os.RemoveAll(path.Join(a.cfg.CacheLocation, tableName))
}

// UpdatesChan returns a channel where async updates to files can be received
func (a *Archiver) UpdatesChan() <-chan Update {
	return a.updatesChan
}
