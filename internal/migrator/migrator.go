package migrator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	jobStatusRunning   = "running"
	jobStatusCompleted = "completed"

	collectionStatusPending = "pending"
	collectionStatusCopying = "copying"
	collectionStatusDone    = "done"
)

type Migrator struct {
	cfg            Config
	sourceClient   *mongo.Client
	targetClient   *mongo.Client
	sourceDB       *mongo.Database
	targetDB       *mongo.Database
	jobsColl       *mongo.Collection
	progressColl   *mongo.Collection
	jobsCollName   string
	progressCollID string
}

type collectionInfo struct {
	Name    string
	Options bson.M
	Count   int64
}

type CollectionProgress struct {
	ID                string      `bson:"_id"`
	JobID             string      `bson:"job_id"`
	Collection        string      `bson:"collection"`
	TotalDocs         int64       `bson:"total_docs"`
	CopiedDocs        int64       `bson:"copied_docs"`
	LastID            interface{} `bson:"last_id,omitempty"`
	Status            string      `bson:"status"`
	Initialized       bool        `bson:"initialized"`
	IndexesCloned     bool        `bson:"indexes_cloned"`
	LastLoggedPercent int         `bson:"last_logged_percent"`
	UpdatedAt         time.Time   `bson:"updated_at"`
	StartedAt         time.Time   `bson:"started_at"`
	CompletedAt       time.Time   `bson:"completed_at,omitempty"`
}

func New(cfg Config) (*Migrator, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sourceOpts, err := buildClientOptions(cfg.Source)
	if err != nil {
		return nil, fmt.Errorf("build source mongo client options: %w", err)
	}

	sourceClient, err := mongo.Connect(ctx, sourceOpts)
	if err != nil {
		return nil, fmt.Errorf("connect source mongo: %w", err)
	}

	targetOpts, err := buildClientOptions(cfg.Target)
	if err != nil {
		_ = sourceClient.Disconnect(context.Background())
		return nil, fmt.Errorf("build target mongo client options: %w", err)
	}

	targetClient, err := mongo.Connect(ctx, targetOpts)
	if err != nil {
		_ = sourceClient.Disconnect(context.Background())
		return nil, fmt.Errorf("connect target mongo: %w", err)
	}

	if err := sourceClient.Ping(ctx, nil); err != nil {
		_ = sourceClient.Disconnect(context.Background())
		_ = targetClient.Disconnect(context.Background())
		return nil, fmt.Errorf("ping source mongo: %w", err)
	}
	if err := targetClient.Ping(ctx, nil); err != nil {
		_ = sourceClient.Disconnect(context.Background())
		_ = targetClient.Disconnect(context.Background())
		return nil, fmt.Errorf("ping target mongo: %w", err)
	}

	jobsName := cfg.MetaCollectionPrefix + "_jobs"
	progressName := cfg.MetaCollectionPrefix + "_collections"

	m := &Migrator{
		cfg:            cfg,
		sourceClient:   sourceClient,
		targetClient:   targetClient,
		sourceDB:       sourceClient.Database(cfg.Source.Database),
		targetDB:       targetClient.Database(cfg.Target.Database),
		jobsColl:       targetClient.Database(cfg.Target.Database).Collection(jobsName),
		progressColl:   targetClient.Database(cfg.Target.Database).Collection(progressName),
		jobsCollName:   jobsName,
		progressCollID: progressName,
	}

	return m, nil
}

func (m *Migrator) Close(ctx context.Context) error {
	var errs []error
	if m.sourceClient != nil {
		if err := m.sourceClient.Disconnect(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if m.targetClient != nil {
		if err := m.targetClient.Disconnect(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (m *Migrator) Run(ctx context.Context) error {
	defer func() {
		if err := m.Close(context.Background()); err != nil {
			log.Printf("failed to disconnect mongo clients: %v", err)
		}
	}()

	if err := m.ensureMetaIndexes(ctx); err != nil {
		return err
	}

	collections, err := m.discoverCollections(ctx)
	if err != nil {
		return err
	}

	if len(collections) == 0 {
		log.Printf("no collections found in source db=%s", m.cfg.Source.Database)
		return nil
	}

	if err := m.upsertJob(ctx, len(collections), jobStatusRunning); err != nil {
		return err
	}

	if err := m.seedCollectionProgress(ctx, collections); err != nil {
		return err
	}

	log.Printf("phase=initialize_collections total=%d", len(collections))
	for _, coll := range collections {
		if err := m.initializeCollection(ctx, coll); err != nil {
			return err
		}
	}

	log.Printf("phase=clone_indexes total=%d", len(collections))
	for _, coll := range collections {
		if err := m.cloneIndexes(ctx, coll.Name); err != nil {
			return err
		}
	}

	log.Printf("phase=copy_documents total=%d", len(collections))
	for _, coll := range collections {
		if err := m.copyCollection(ctx, coll.Name); err != nil {
			return err
		}
		if err := m.updateCompletedCount(ctx); err != nil {
			return err
		}
	}

	if err := m.upsertJob(ctx, len(collections), jobStatusCompleted); err != nil {
		return err
	}

	log.Printf("migration complete job_id=%s source=%s target=%s", m.cfg.JobID, m.cfg.Source.Database, m.cfg.Target.Database)
	return nil
}

func (m *Migrator) ensureMetaIndexes(ctx context.Context) error {
	_, err := m.jobsColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "status", Value: 1}}},
		{Keys: bson.D{{Key: "updated_at", Value: 1}}},
	})
	if err != nil {
		return fmt.Errorf("create jobs indexes: %w", err)
	}

	_, err = m.progressColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "job_id", Value: 1}, {Key: "status", Value: 1}}},
		{Keys: bson.D{{Key: "updated_at", Value: 1}}},
	})
	if err != nil {
		return fmt.Errorf("create progress indexes: %w", err)
	}

	return nil
}

func (m *Migrator) discoverCollections(ctx context.Context) ([]collectionInfo, error) {
	cur, err := m.sourceDB.ListCollections(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list source collections: %w", err)
	}
	defer cur.Close(ctx)

	metaNames := map[string]struct{}{
		m.jobsCollName:   {},
		m.progressCollID: {},
	}

	collections := make([]collectionInfo, 0)
	for cur.Next(ctx) {
		var raw bson.M
		if err := cur.Decode(&raw); err != nil {
			return nil, fmt.Errorf("decode listCollections item: %w", err)
		}

		name, _ := raw["name"].(string)
		typeName, _ := raw["type"].(string)
		if name == "" || typeName != "collection" {
			continue
		}
		if strings.HasPrefix(name, "system.") {
			continue
		}
		if _, ok := metaNames[name]; ok {
			continue
		}

		opts, _ := raw["options"].(bson.M)
		count, err := m.sourceDB.Collection(name).CountDocuments(ctx, bson.D{})
		if err != nil {
			return nil, fmt.Errorf("count documents for %s: %w", name, err)
		}

		collections = append(collections, collectionInfo{
			Name:    name,
			Options: opts,
			Count:   count,
		})
	}

	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("iterate listCollections: %w", err)
	}

	sort.Slice(collections, func(i, j int) bool {
		return collections[i].Name < collections[j].Name
	})

	for _, c := range collections {
		log.Printf("discovered collection=%s total_docs=%d", c.Name, c.Count)
	}

	return collections, nil
}

func (m *Migrator) upsertJob(ctx context.Context, totalCollections int, status string) error {
	now := time.Now().UTC()
	filter := bson.M{"_id": m.cfg.JobID}
	update := bson.M{
		"$set": bson.M{
			"source_db":         m.cfg.Source.Database,
			"target_db":         m.cfg.Target.Database,
			"status":            status,
			"total_collections": totalCollections,
			"updated_at":        now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}
	if status == jobStatusCompleted {
		update["$set"].(bson.M)["completed_at"] = now
	}

	_, err := m.jobsColl.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("upsert job progress: %w", err)
	}
	return nil
}

func (m *Migrator) seedCollectionProgress(ctx context.Context, collections []collectionInfo) error {
	now := time.Now().UTC()
	for _, coll := range collections {
		id := collectionProgressID(m.cfg.JobID, coll.Name)
		_, err := m.progressColl.UpdateOne(
			ctx,
			bson.M{"_id": id},
			bson.M{
				"$set": bson.M{
					"job_id":     m.cfg.JobID,
					"collection": coll.Name,
					"total_docs": coll.Count,
					"updated_at": now,
				},
				"$setOnInsert": bson.M{
					"copied_docs":         0,
					"status":              collectionStatusPending,
					"initialized":         false,
					"indexes_cloned":      false,
					"last_logged_percent": 0,
					"started_at":          now,
				},
			},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			return fmt.Errorf("seed progress for collection %s: %w", coll.Name, err)
		}
	}
	return nil
}

func (m *Migrator) initializeCollection(ctx context.Context, coll collectionInfo) error {
	progress, err := m.getCollectionProgress(ctx, coll.Name)
	if err != nil {
		return err
	}
	if progress.Initialized {
		return nil
	}

	names, err := m.targetDB.ListCollectionNames(ctx, bson.M{"name": coll.Name})
	if err != nil {
		return fmt.Errorf("check target collection %s: %w", coll.Name, err)
	}

	if len(names) == 0 {
		cmd := bson.D{{Key: "create", Value: coll.Name}}
		for k, v := range coll.Options {
			cmd = append(cmd, bson.E{Key: k, Value: v})
		}
		if err := m.targetDB.RunCommand(ctx, cmd).Err(); err != nil {
			if !isNamespaceExistsError(err) {
				return fmt.Errorf("create target collection %s: %w", coll.Name, err)
			}
		}
	}

	_, err = m.progressColl.UpdateOne(ctx, bson.M{"_id": progress.ID}, bson.M{"$set": bson.M{
		"initialized": true,
		"updated_at":  time.Now().UTC(),
	}})
	if err != nil {
		return fmt.Errorf("mark initialized for %s: %w", coll.Name, err)
	}
	return nil
}

func (m *Migrator) cloneIndexes(ctx context.Context, collection string) error {
	progress, err := m.getCollectionProgress(ctx, collection)
	if err != nil {
		return err
	}
	if progress.IndexesCloned {
		return nil
	}

	cur, err := m.sourceDB.Collection(collection).Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list indexes for %s: %w", collection, err)
	}
	defer cur.Close(ctx)

	indexes := make([]interface{}, 0)
	for cur.Next(ctx) {
		var idx bson.M
		if err := cur.Decode(&idx); err != nil {
			return fmt.Errorf("decode index for %s: %w", collection, err)
		}
		name, _ := idx["name"].(string)
		if name == "_id_" {
			continue
		}
		delete(idx, "v")
		delete(idx, "ns")
		indexes = append(indexes, idx)
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("iterate indexes for %s: %w", collection, err)
	}

	if len(indexes) > 0 {
		cmd := bson.D{
			{Key: "createIndexes", Value: collection},
			{Key: "indexes", Value: indexes},
		}
		if err := m.targetDB.RunCommand(ctx, cmd).Err(); err != nil {
			return fmt.Errorf("create indexes for %s: %w", collection, err)
		}
	}

	_, err = m.progressColl.UpdateOne(ctx, bson.M{"_id": progress.ID}, bson.M{"$set": bson.M{
		"indexes_cloned": true,
		"updated_at":     time.Now().UTC(),
	}})
	if err != nil {
		return fmt.Errorf("mark indexes cloned for %s: %w", collection, err)
	}
	return nil
}

func (m *Migrator) copyCollection(ctx context.Context, collection string) error {
	progress, err := m.getCollectionProgress(ctx, collection)
	if err != nil {
		return err
	}
	if progress.Status == collectionStatusDone {
		log.Printf("skip collection=%s reason=already_done", collection)
		return nil
	}

	sourceColl := m.sourceDB.Collection(collection)
	targetColl := m.targetDB.Collection(collection)

	for {
		filter := bson.D{}
		if progress.LastID != nil {
			filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: progress.LastID}}}}
		}

		opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(m.cfg.BatchSize))
		cur, err := sourceColl.Find(ctx, filter, opts)
		if err != nil {
			return fmt.Errorf("find batch collection=%s: %w", collection, err)
		}

		docs := make([]bson.M, 0, m.cfg.BatchSize)
		if err := cur.All(ctx, &docs); err != nil {
			_ = cur.Close(ctx)
			return fmt.Errorf("decode batch collection=%s: %w", collection, err)
		}
		_ = cur.Close(ctx)

		if len(docs) == 0 {
			now := time.Now().UTC()
			_, err := m.progressColl.UpdateOne(ctx, bson.M{"_id": progress.ID}, bson.M{"$set": bson.M{
				"status":       collectionStatusDone,
				"updated_at":   now,
				"completed_at": now,
			}})
			if err != nil {
				return fmt.Errorf("mark collection done %s: %w", collection, err)
			}
			log.Printf("collection=%s progress=100%% (%d/%d)", collection, progress.CopiedDocs, progress.TotalDocs)
			return nil
		}

		insertDocs := make([]interface{}, 0, len(docs))
		for _, doc := range docs {
			insertDocs = append(insertDocs, doc)
		}

		_, err = targetColl.InsertMany(ctx, insertDocs, options.InsertMany().SetOrdered(false))
		if err != nil && !isIgnorableDuplicateError(err) {
			return fmt.Errorf("insert batch collection=%s: %w", collection, err)
		}

		lastID := docs[len(docs)-1]["_id"]
		progress.LastID = lastID
		progress.CopiedDocs += int64(len(docs))
		if progress.TotalDocs > 0 && progress.CopiedDocs > progress.TotalDocs {
			progress.CopiedDocs = progress.TotalDocs
		}

		now := time.Now().UTC()
		_, err = m.progressColl.UpdateOne(ctx, bson.M{"_id": progress.ID}, bson.M{"$set": bson.M{
			"status":      collectionStatusCopying,
			"last_id":     progress.LastID,
			"copied_docs": progress.CopiedDocs,
			"updated_at":  now,
		}})
		if err != nil {
			return fmt.Errorf("update checkpoint collection=%s: %w", collection, err)
		}

		loggedPercent, err := m.logProgressIfNeeded(ctx, progress)
		if err != nil {
			return err
		}
		progress.LastLoggedPercent = loggedPercent
	}
}

func (m *Migrator) logProgressIfNeeded(ctx context.Context, progress CollectionProgress) (int, error) {
	if progress.TotalDocs == 0 {
		if progress.LastLoggedPercent < 100 {
			log.Printf("collection=%s progress=100%% (0/0)", progress.Collection)
			_, err := m.progressColl.UpdateOne(ctx, bson.M{"_id": progress.ID}, bson.M{"$set": bson.M{
				"last_logged_percent": 100,
				"updated_at":          time.Now().UTC(),
			}})
			if err != nil {
				return progress.LastLoggedPercent, fmt.Errorf("update progress log marker %s: %w", progress.Collection, err)
			}
			return 100, nil
		}
		return progress.LastLoggedPercent, nil
	}

	percent := int((progress.CopiedDocs * 100) / progress.TotalDocs)
	if percent > 100 {
		percent = 100
	}

	lastLogged := progress.LastLoggedPercent
	nextThreshold := ((lastLogged / m.cfg.LogPercentStep) + 1) * m.cfg.LogPercentStep
	if percent < 100 && percent < nextThreshold {
		return lastLogged, nil
	}

	milestone := (percent / m.cfg.LogPercentStep) * m.cfg.LogPercentStep
	if percent == 100 {
		milestone = 100
	}
	if milestone <= lastLogged {
		return lastLogged, nil
	}

	log.Printf("collection=%s progress=%d%% (%d/%d)", progress.Collection, milestone, progress.CopiedDocs, progress.TotalDocs)
	_, err := m.progressColl.UpdateOne(ctx, bson.M{"_id": progress.ID}, bson.M{"$set": bson.M{
		"last_logged_percent": milestone,
		"updated_at":          time.Now().UTC(),
	}})
	if err != nil {
		return lastLogged, fmt.Errorf("update progress log marker %s: %w", progress.Collection, err)
	}

	return milestone, nil
}

func (m *Migrator) getCollectionProgress(ctx context.Context, collection string) (CollectionProgress, error) {
	id := collectionProgressID(m.cfg.JobID, collection)
	var progress CollectionProgress
	if err := m.progressColl.FindOne(ctx, bson.M{"_id": id}).Decode(&progress); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return CollectionProgress{}, fmt.Errorf("progress not initialized for collection %s", collection)
		}
		return CollectionProgress{}, fmt.Errorf("load progress for collection %s: %w", collection, err)
	}
	return progress, nil
}

func (m *Migrator) updateCompletedCount(ctx context.Context) error {
	doneCount, err := m.progressColl.CountDocuments(ctx, bson.M{"job_id": m.cfg.JobID, "status": collectionStatusDone})
	if err != nil {
		return fmt.Errorf("count completed collections: %w", err)
	}
	_, err = m.jobsColl.UpdateOne(ctx, bson.M{"_id": m.cfg.JobID}, bson.M{"$set": bson.M{
		"completed_collections": doneCount,
		"updated_at":            time.Now().UTC(),
	}})
	if err != nil {
		return fmt.Errorf("update completed collections count: %w", err)
	}
	return nil
}

func collectionProgressID(jobID, collection string) string {
	return jobID + "::" + collection
}

func isNamespaceExistsError(err error) bool {
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		return cmdErr.Code == 48
	}
	return false
}

func isIgnorableDuplicateError(err error) bool {
	var bulkErr mongo.BulkWriteException
	if errors.As(err, &bulkErr) {
		if bulkErr.WriteConcernError != nil {
			return false
		}
		if len(bulkErr.WriteErrors) == 0 {
			return false
		}
		for _, w := range bulkErr.WriteErrors {
			if w.Code != 11000 {
				return false
			}
		}
		return true
	}

	var writeErr mongo.WriteException
	if errors.As(err, &writeErr) {
		if writeErr.WriteConcernError != nil {
			return false
		}
		if len(writeErr.WriteErrors) == 0 {
			return false
		}
		for _, w := range writeErr.WriteErrors {
			if w.Code != 11000 {
				return false
			}
		}
		return true
	}

	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		return cmdErr.Code == 11000
	}

	return false
}
