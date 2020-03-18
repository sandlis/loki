package local

import (
	"context"
	"sync"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"go.etcd.io/bbolt"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/local"
	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
)

type archivedDB struct {
	boltDBs map[string]*bbolt.DB
	sync.RWMutex
}

type BoltdbIndexClientWithArchiver struct {
	*local.BoltIndexClient

	archiver       *Archiver
	archivedDbsMtx sync.RWMutex
	archivedDbs    map[string]*archivedDB

	done chan struct{}
	wait sync.WaitGroup
}

// NewBoltDBIndexClient creates a new IndexClient that used BoltDB.
func NewBoltDBIndexClient(cfg local.BoltDBConfig, archiver *Archiver) (chunk.IndexClient, error) {
	boltDBIndexClient, err := local.NewBoltDBIndexClient(cfg)
	if err != nil {
		return nil, err
	}

	indexClient := BoltdbIndexClientWithArchiver{
		BoltIndexClient: boltDBIndexClient,
		archivedDbs:     map[string]*archivedDB{},
		done:            make(chan struct{}),
	}

	if archiver != nil {
		indexClient.archiver = archiver
		go indexClient.processArchiverUpdates()
	}

	return &indexClient, nil
}

func (b *BoltdbIndexClientWithArchiver) Stop() {
	close(b.done)
	b.BoltIndexClient.Stop()

	if b.archiver != nil {
		b.archiver.Stop()
	}
	b.wait.Wait()
}

func (b *BoltdbIndexClientWithArchiver) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) (shouldContinue bool)) error {
	return chunk_util.DoParallelQueries(ctx, b.query, queries, callback)
}

func (b *BoltdbIndexClientWithArchiver) query(ctx context.Context, query chunk.IndexQuery, callback func(chunk.ReadBatch) (shouldContinue bool)) error {
	db, err := b.GetDB(query.TableName, local.DBOperationRead)
	if err != nil {
		return err
	}

	var aDB *archivedDB
	if b.archiver != nil {
		aDB, err = b.getArchivedDB(ctx, query.TableName)
		if err != nil {
			return err
		}

		aDB.RLock()
		defer aDB.RUnlock()
	}

	if db != nil {
		if err := b.QueryDB(ctx, db, query, callback); err != nil {
			return err
		}
	}

	if aDB != nil {
		for _, db := range aDB.boltDBs {
			if err := b.QueryDB(ctx, db, query, callback); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BoltdbIndexClientWithArchiver) getArchivedDB(ctx context.Context, name string) (*archivedDB, error) {
	b.archivedDbsMtx.RLock()
	aDB, ok := b.archivedDbs[name]
	b.archivedDbsMtx.RUnlock()
	if ok {
		return aDB, nil
	}

	b.archivedDbsMtx.Lock()
	defer b.archivedDbsMtx.Unlock()

	aDB, ok = b.archivedDbs[name]
	if ok {
		return aDB, nil
	}

	archivedFilePaths, err := b.archiver.Sync(ctx, name)
	if err != nil {
		return nil, err
	}

	aDB = &archivedDB{}

	boltDBs := make(map[string]*bbolt.DB, len(archivedFilePaths))
	for _, archivedFilePath := range archivedFilePaths {
		db, err := local.OpenBoltdbFile(archivedFilePath)
		if err != nil {
			return nil, err
		}

		boltDBs[archivedFilePath] = db
	}
	aDB.boltDBs = boltDBs
	b.archivedDbs[name] = aDB

	return aDB, nil
}

func (b *BoltdbIndexClientWithArchiver) processArchiverUpdates() {
	b.wait.Add(1)
	defer b.wait.Done()

	updatesChan := b.archiver.UpdatesChan()
	for {
		select {
		case update := <-updatesChan:
			switch update.UpdateType {
			case UpdateTypeFileRemoved:
				err := b.processArchiverFileRemovedUpdate(context.Background(), update.TableName, update.FilePath)
				if err != nil {
					level.Error(util.Logger).Log("msg", "failed to process file delete update from archiver", "update", update, "err", err)
				}
			case UpdateTypeFileDownloaded:
				err := b.processArchiverFileDownloadedUpdate(context.Background(), update.TableName, update.FilePath)
				if err != nil {
					level.Error(util.Logger).Log("msg", "failed to process file added update from archiver", "update", update, "err", err)
				}
			case UpdateTypeTableRemoved:
				err := b.processArchiverTableDeletedUpdate(update.TableName)
				if err != nil {
					level.Error(util.Logger).Log("msg", "failed to process table deleted update from archiver", "update", update, "err", err)
				}
			default:
				level.Error(util.Logger).Log("Invalid update type from archiver", "update", update)
			}
		case <-b.done:
			return
		}
	}
}

func (b *BoltdbIndexClientWithArchiver) processArchiverFileRemovedUpdate(ctx context.Context, tableName, filePath string) error {
	aDB, err := b.getArchivedDB(ctx, tableName)
	if err != nil {
		return err
	}

	aDB.Lock()
	defer aDB.Unlock()

	boltDB, isOK := aDB.boltDBs[filePath]
	if !isOK {
		return nil
	}

	delete(aDB.boltDBs, filePath)

	if err := boltDB.Close(); err != nil {
		return err
	}

	return nil
}

func (b *BoltdbIndexClientWithArchiver) processArchiverFileDownloadedUpdate(ctx context.Context, tableName, filePath string) error {
	aDB, err := b.getArchivedDB(ctx, tableName)
	if err != nil {
		return err
	}

	aDB.Lock()
	defer aDB.Unlock()
	_, isOK := aDB.boltDBs[filePath]
	if isOK {
		return nil
	}

	boltDB, err := local.OpenBoltdbFile(filePath)
	if err != nil {
		return err
	}

	aDB.boltDBs[filePath] = boltDB
	return nil
}

func (b *BoltdbIndexClientWithArchiver) processArchiverTableDeletedUpdate(tableName string) error {
	b.archivedDbsMtx.Lock()
	defer b.archivedDbsMtx.Unlock()

	aDB, isOK := b.archivedDbs[tableName]
	if !isOK {
		return nil
	}

	aDB.Lock()
	defer aDB.Unlock()

	for _, boltDB := range aDB.boltDBs {
		err := boltDB.Close()
		if err != nil {
			level.Error(util.Logger).Log("msg", "failed to close removed boltdb", "filepath", boltDB.Path(), "err", err)
		}
	}

	delete(b.archivedDbs, tableName)
	return nil
}
