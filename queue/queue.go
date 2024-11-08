package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

const (
	// DefaultMaxWorker is the default maximum number of jobs that can be processed in parallel.
	DefaultMaxWorker uint64 = 5

	// DefaultAckDeadline is the default number of seconds a job can wait to be acknowledged after being queued.
	DefaultAckDeadline uint64 = 60

	// DefaultMaxExecutionTime is the default maximum duration (in seconds) allowed for a job or process to complete.
	DefaultMaxExecutionTime uint64 = 0

	redisInProgressKeyFormat        = "in_progress_queue_in_project_id:%s"
	redisQueueNumberKeyFormat       = "queue_number_in_project_id:%s"
	redisQueueKeyFormat             = "queue_in_project_id:%s"
	redisJobInfoKeyFormat           = "job_info_queue_in_project_%s:%s"
	redisAckDeadlineKeyFormat       = "ack_deadline_in_project_%s:%s"
	redisExecutionDeadlineKeyFormat = "execution_deadline_in_project_%s:%s"

	statusCompleted  = "completed"
	statusProcessing = "processing"
	statusSkipped    = "skipped"
	statusAssigned   = "assigned"
	statusQueued     = "queued"

	maxRetriesFromRedisTx = 3
)

// ErrJobNotInQueue indicates that the job is not in the queue.
var ErrJobNotInQueue = errors.New("job not in queue")

// ErrLockNotAcquired indicates that another instance is already acquiring the lock.
var ErrLockNotAcquired = errors.New("could not acquire lock")

// Registry manages dynamic creation and reuse of Queue instances for different projects with distributed locking.
type Registry struct {
	queues     map[string]*Queue
	locker     *redislock.Client
	localMutex sync.RWMutex
}

// NewRegistry returns a new Registry instance using redis for distributed synchronization.
func NewRegistry(redis *redis.Client) (*Registry, error) {
	locker := redislock.New(redis)
	return &Registry{
		queues: make(map[string]*Queue),
		locker: locker,
	}, nil
}

// GetQueue gets or creates a Queue instance for the given project ID,
// using distributed lock to ensure that only one Queue instance is created for each project.
func (r *Registry) GetOrCreateQueue(ctx context.Context, config *Config) (*Queue, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	projectID := config.ProjectID
	queueLockKey := fmt.Sprintf("queue_lock:%s", projectID)

	r.localMutex.RLock()
	q, exists := r.queues[projectID]
	r.localMutex.RUnlock()
	if exists {
		return q, nil
	}

	lock, err := r.locker.Obtain(ctx, queueLockKey, time.Second*5, nil)
	if errors.Is(err, redislock.ErrNotObtained) {
		log.Printf("[INFO] Another instance is initializing queue for project: %s\n", projectID)
		return nil, ErrLockNotAcquired
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := lock.Release(ctx); err != nil {
			log.Printf("[WARN] Failed to release the lock for project: %s\n", projectID)
		}
	}()

	r.localMutex.Lock()
	defer r.localMutex.Unlock()

	queue, exits := r.queues[projectID]
	if exits {
		log.Printf("[INFO] Queue already created by another instance during lock for project: %s\n", projectID)
		return queue, nil
	}

	newQ, err := newQueue(ctx, config)
	if err != nil {
		return nil, err
	}

	r.queues[projectID] = newQ

	log.Printf("[INFO] Queue created for project: %s\n", projectID)
	return newQ, nil
}

type Config struct {
	Redis *redis.Client

	// ProjectID is the ID of the project that the queue belongs to.
	ProjectID string

	// The maximum number of jobs that can be processed in parallel.
	//
	// If set to 0, there would use the default value.
	//
	// By default, there is 5 jobs in parallel.
	MaxWorker uint64

	// The number of seconds a job can wait to be acknowledged after being queued.
	// If the job is not acknowledged within this time, it will be considered as skipped.
	//
	// If set to 0, there would use the default value.
	//
	// By default, there is 60 seconds.
	AckDeadline uint64

	// MaxExecutionTime defines the maximum duration (in seconds) allowed for a job or process to complete.
	//
	// If set to 0, there is no time limit, and the job can run indefinitely.
	//
	// By default, there is no time limit.
	MaxExecutionTime uint64

	// BatchSize defines the maximum number of rows to process in backup mode.
	//
	// Set to 0 to disable backup mode.
	//
	// By default, there is no backup mode.
	BatchSize uint64

	// The secret used to make the signature of the message.
	Secret []byte

	// Backup is the function used to backup the queue when cleaning up.
	//
	// By default, it does nothing.
	Backup func(ctx context.Context, data []JobBackup) error

	// Publisher is the function used to publish the job when dequeueing, skipping, and completing.
	//
	// By default, it does nothing.
	Publisher func(ctx context.Context, job *Job) error
}

// New returns a new Queue instance with the given configuration.
func newQueue(ctx context.Context, config *Config) (*Queue, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	q := &Queue{
		redis:            config.Redis,
		maxWorker:        defaultIfZero(config.MaxWorker, DefaultMaxWorker),
		ackDeadline:      defaultIfZero(config.AckDeadline, DefaultAckDeadline),
		maxExecutionTime: config.MaxExecutionTime,
		secret:           config.Secret,
		projectID:        config.ProjectID,
		batchSize:        config.BatchSize,
		backup:           config.Backup,
		publisher:        config.Publisher,
	}

	q.startBackgroundTasks(ctx)
	return q, nil
}

func validateConfig(config *Config) error {
	if config == nil {
		return errors.New("config is nil")
	}
	if config.Redis == nil {
		return errors.New("redis is nil")
	}
	if config.Secret == nil {
		return errors.New("secret is nil")
	}
	if config.ProjectID == "" {
		return errors.New("project id is empty")
	}

	return nil
}

func defaultIfZero(value, defaultValue uint64) uint64 {
	if value == 0 {
		return defaultValue
	}
	return value
}

type Queue struct {
	redis            *redis.Client
	secret           []byte
	projectID        string
	maxWorker        uint64
	maxExecutionTime uint64
	ackDeadline      uint64
	batchSize        uint64
	backup           func(ctx context.Context, data []JobBackup) error
	publisher        func(ctx context.Context, job *Job) error
}

func (q *Queue) startBackgroundTasks(ctx context.Context) {
	go func() {
		log.Println("[INFO] Starting task notifications subscriber from redis expiration events for project: ", q.projectID)
		q.subscribeToKeySpaceNotifications(ctx)
	}()

	go func() {
		log.Println("[INFO] Starting job queue monitor for project: ", q.projectID)
		q.monitorQueueAndDequeue(ctx)
	}()
}

func (q *Queue) GetMaxWorker() uint64 {
	return q.maxWorker
}

func (q *Queue) GetProjectID() string {
	return q.projectID
}

func (q *Queue) GetLength(ctx context.Context) (uint64, error) {
	projectKey := fmt.Sprintf(redisQueueKeyFormat, q.projectID)
	length, err := q.redis.LLen(ctx, projectKey).Result()
	if err != nil {
		return 0, err
	}
	return uint64(length), nil
}

func (q *Queue) GetPosition(ctx context.Context, jobID string) (uint64, error) {
	job, err := q.GetJob(ctx, jobID)
	if err != nil {
		return 0, err
	}
	if job.Status != statusQueued {
		return 0, nil
	}

	projectKey := fmt.Sprintf(redisQueueKeyFormat, q.projectID)
	jobs, err := q.redis.LRange(ctx, projectKey, 0, -1).Result()
	if err != nil {
		return 0, err
	}

	targetIndex := -1
	f := func() error {
		for i, id := range jobs {
			if id == jobID {
				targetIndex = i
				break
			}
		}
		if targetIndex == -1 {
			return ErrJobNotInQueue
		}

		return nil
	}

	if err := retryIf(maxRetriesFromRedisTx, time.Second, isRetryable, f); err != nil {
		return 0, err
	}

	return uint64(targetIndex + 1), nil
}

func (q *Queue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	jobKey := fmt.Sprintf(redisJobInfoKeyFormat, q.projectID, jobID)

	var job Job
	f := func() error {
		err := q.redis.HGetAll(ctx, jobKey).Scan(&job)
		if err == redis.Nil {
			return ErrJobNotInQueue
		}
		if err != nil {
			return err
		}

		return nil
	}

	if err := retryIf(maxRetriesFromRedisTx, time.Second, isRetryable, f); err != nil {
		return nil, err
	}

	return &job, nil
}

func (q *Queue) Ack(ctx context.Context, jobID string) error {
	jobKey := fmt.Sprintf(redisJobInfoKeyFormat, q.projectID, jobID)
	ackKey := fmt.Sprintf(redisAckDeadlineKeyFormat, q.projectID, jobID)

	f := func() error {
		return q.redis.Watch(ctx, func(tx *redis.Tx) error {
			status, err := tx.HGet(ctx, jobKey, "status").Result()
			if err == redis.Nil {
				return fmt.Errorf("Job %s in this project %s does not exist", jobID, q.projectID)
			}
			if err != nil {
				return err
			}
			if status == statusProcessing {
				return fmt.Errorf("Job %s in this project %s has already been processed", jobID, q.projectID)
			}
			if status != statusAssigned {
				return fmt.Errorf("Job %s in this project %s is not currently being assigned", jobID, q.projectID)
			}

			_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.HSet(ctx, jobKey, "status", statusProcessing, "acked_at", time.Now())
				pipe.Del(ctx, ackKey)
				return nil
			})
			if err != nil {
				return fmt.Errorf("Ack(TxPipelined): %w", err)
			}

			return nil
		}, jobID, ackKey)
	}

	if err := retryIf(maxRetriesFromRedisTx, time.Second, isRetryable, f); err != nil {
		return err
	}

	log.Printf("[INFO] Job %s in this project %s has been acknowledged", jobID, q.projectID)
	return nil
}

func (q *Queue) Done(ctx context.Context, jobID string) error {
	jobKey := fmt.Sprintf(redisJobInfoKeyFormat, q.projectID, jobID)
	inProgressKey := fmt.Sprintf(redisInProgressKeyFormat, q.projectID)
	executionKey := fmt.Sprintf(redisExecutionDeadlineKeyFormat, q.projectID, jobID)
	f := func() error {
		return q.redis.Watch(ctx, func(tx *redis.Tx) error {
			status, err := tx.HGet(ctx, jobKey, "status").Result()
			if err == redis.Nil {
				return fmt.Errorf("Job %s in this project %s does not exist", jobID, q.projectID)
			}
			if err != nil {
				return fmt.Errorf("Failed to get the job: %v", err)
			}
			if status == statusCompleted {
				return fmt.Errorf("Job %s in this project %s has already been completed", jobID, q.projectID)
			}
			if status != statusProcessing {
				return fmt.Errorf("Job %s in this project %s is not currently being processed", jobID, q.projectID)
			}

			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.HSet(ctx, jobKey, "status", statusCompleted, "done_at", time.Now())
				pipe.Decr(ctx, inProgressKey)
				pipe.Del(ctx, executionKey)

				return nil
			})
			if err != nil {
				return fmt.Errorf("Done(TxPipelined): %w", err)
			}

			var job Job
			if err := tx.HGetAll(ctx, jobKey).Scan(&job); err != nil {
				log.Printf("[ERROR] tx.HGetAll: %v", err)
			}
			if err := q.publisher(ctx, &job); err != nil {
				log.Printf("[ERROR] publisher(Done): %v", err)
			}

			return nil
		}, jobKey, inProgressKey, executionKey)
	}

	if err := retryIf(maxRetriesFromRedisTx, time.Second, isRetryable, f); err != nil {
		return err
	}

	log.Printf("[INFO] Job %s in this project %s has been done", jobID, q.projectID)

	if err := q.dequeue(ctx); err != nil {
		return fmt.Errorf("Done(dequeue): %w", err)
	}

	return nil
}

type Job struct {
	QueueNumber uint64    `redis:"queue_number" json:"queueNumber"`
	ID          string    `redis:"job_id" json:"jobId"`
	ProjectID   string    `redis:"project_id" json:"projectId"`
	Status      string    `redis:"status" json:"status"`
	JoinedAt    time.Time `redis:"joined_at" json:"joinedAt"`
}

func (q *Queue) Join(ctx context.Context, jobID string) (*Job, error) {
	var job Job
	jobKey := fmt.Sprintf(redisJobInfoKeyFormat, q.projectID, jobID)
	queueNumberKey := fmt.Sprintf(redisQueueNumberKeyFormat, q.projectID)
	projectKey := fmt.Sprintf(redisQueueKeyFormat, q.projectID)

	f := func() error {
		return q.redis.Watch(ctx, func(tx *redis.Tx) error {
			exists, err := tx.Exists(ctx, jobKey).Result()
			if err != nil {
				return err
			}
			if exists > 0 {
				return fmt.Errorf("Job %s already exists in this project %s (or may it has been processed or skipped)", q.projectID, jobID)
			}

			cmds, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.RPush(ctx, projectKey, jobID)
				pipe.Incr(ctx, queueNumberKey)
				return nil
			})
			if err != nil {
				return fmt.Errorf("Join(TxPipelined): %v", err)
			}

			queueNumber, err := cmds[1].(*redis.IntCmd).Result()
			if err != nil {
				return fmt.Errorf("Failed to increment queue number: %v", err)
			}

			now := time.Now()
			job.ID = jobID
			job.QueueNumber = uint64(queueNumber)
			job.ProjectID = q.projectID
			job.Status = statusQueued
			job.JoinedAt = now
			tx.HSet(ctx, jobKey, job)

			return nil
		}, projectKey, queueNumberKey)
	}

	if err := retryIf(maxRetriesFromRedisTx, time.Second, isRetryable, f); err != nil {
		return nil, err
	}

	log.Printf("[INFO] Job ID %s with queue number %d has successfully joined the queue in project %s.", job.ID, job.QueueNumber, q.projectID)
	return &job, nil
}

type JobBackup struct {
	AckTimeoutInSeconds       int `redis:"ack_timeout_sec"`
	ExecutionTimeoutInSeconds int `redis:"execution_timeout_sec"`

	QueueNumber uint64    `redis:"queue_number"`
	ID          string    `redis:"job_id"`
	ProjectID   string    `redis:"project_id"`
	Status      string    `redis:"status"`
	JoinedAt    time.Time `redis:"joined_at"`

	AssignedAt *time.Time `redis:"assigned_at"`
	AckedAt    *time.Time `redis:"acked_at"`
	DoneAt     *time.Time `redis:"done_at"`

	SkippedAt *time.Time `redis:"skipped_at"`
	Reason    string     `redis:"reason"`
}

func (q *Queue) Cleanup(ctx context.Context) error {
	inProgressKey := fmt.Sprintf(redisInProgressKeyFormat, q.projectID)
	queueKey := fmt.Sprintf(redisQueueKeyFormat, q.projectID)
	queueNumberKey := fmt.Sprintf(redisQueueNumberKeyFormat, q.projectID)
	ackDeadlineKey := fmt.Sprintf(redisAckDeadlineKeyFormat, q.projectID, "*")
	executionDeadlineKey := fmt.Sprintf(redisExecutionDeadlineKeyFormat, q.projectID, "*")
	jobInfoKey := fmt.Sprintf(redisJobInfoKeyFormat, q.projectID, "*")

	keys := make([]string, 0)
	for _, pattern := range []string{ackDeadlineKey, executionDeadlineKey, jobInfoKey} {
		var cursor uint64
		for {
			ks, nextCursor, err := q.redis.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				log.Printf("[ERROR] Failed to scan keys: %v", err)
				continue
			}

			if len(ks) > 0 {
				keys = append(keys, ks...)
			}
			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
	}

	if q.batchSize > 0 {
		var cursor uint64
		for {
			keys, nextCursor, err := q.redis.Scan(ctx, cursor, jobInfoKey, int64(q.batchSize)).Result()
			if err != nil {
				return fmt.Errorf("redis.Scan: %v", err)
			}

			jobs := make([]JobBackup, 0)
			for _, k := range keys {
				var job JobBackup
				err = q.redis.HGetAll(ctx, k).Scan(&job)
				if err != nil {
					return fmt.Errorf("redis.HGetAll: %s %w", k, err)
				}
				jobs = append(jobs, job)
			}

			if err := q.backup(ctx, jobs); err != nil {
				return fmt.Errorf("Failed to backup queue: %v", err)
			}

			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}

		log.Printf("[INFO] All jobs for project %s have been backed up.", q.projectID)
	}

	q.redis.Del(ctx, inProgressKey)
	q.redis.Del(ctx, queueKey)
	q.redis.Del(ctx, queueNumberKey)
	q.redis.Del(ctx, keys...)

	log.Printf("[INFO] All keys and queues for project %s have been cleared.", q.projectID)
	return nil
}

func (q *Queue) incrementInProgress(ctx context.Context) (uint64, error) {
	return q.redis.Incr(ctx, fmt.Sprintf(redisInProgressKeyFormat, q.projectID)).Uint64()
}

func (q *Queue) decrementInProgress(ctx context.Context) (uint64, error) {
	return q.redis.Decr(ctx, fmt.Sprintf(redisInProgressKeyFormat, q.projectID)).Uint64()
}

func (q *Queue) countInProgress(ctx context.Context) (uint64, error) {
	count, err := q.redis.Get(ctx, fmt.Sprintf(redisInProgressKeyFormat, q.projectID)).Uint64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (q *Queue) dequeue(ctx context.Context) error {
	inProgressKey := fmt.Sprintf(redisInProgressKeyFormat, q.projectID)
	projectKey := fmt.Sprintf(redisQueueKeyFormat, q.projectID)

	f := func() error {
		return q.redis.Watch(ctx, func(tx *redis.Tx) error {
			isEmpty, err := q.isEmpty(ctx)
			if err != nil {
				return err
			}
			if isEmpty {
				return nil
			}

			inProgressCount, err := tx.Get(ctx, inProgressKey).Uint64()
			if err != nil && err != redis.Nil {
				return err
			}
			if inProgressCount >= q.maxWorker {
				// log.Printf("[WARN] %d jobs already in progress, waiting for one to complete", q.maxWorker)
				return nil
			}

			cmds, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.LPop(ctx, projectKey)
				pipe.Incr(ctx, inProgressKey)
				return nil
			})
			if err != nil {
				return fmt.Errorf("dequeue(TxPipelined): %w", err)
			}

			jobID, err := cmds[0].(*redis.StringCmd).Result()
			if err == redis.Nil {
				log.Println("[WARN] No job available in queue")
				return nil
			}
			if err != nil {
				return fmt.Errorf("Failed to pop job from queue: %w", err)
			}

			jobKey := fmt.Sprintf(redisJobInfoKeyFormat, q.projectID, jobID)
			ackDeadlineKey := fmt.Sprintf(redisAckDeadlineKeyFormat, q.projectID, jobID)
			executionDeadlineKey := fmt.Sprintf(redisExecutionDeadlineKeyFormat, q.projectID, jobID)

			tx.HSet(ctx, jobKey, map[string]any{
				"status":                statusAssigned,
				"ack_timeout_sec":       q.ackDeadline,
				"execution_timeout_sec": q.maxExecutionTime,
				"assigned_at":           time.Now(),
			})

			tx.Set(ctx, ackDeadlineKey, "Ack deadline", time.Duration(q.ackDeadline)*time.Second)
			if q.maxExecutionTime > 0 {
				tx.Set(ctx, executionDeadlineKey, "Execution deadline", time.Duration(q.maxExecutionTime)*time.Second)
			}

			var job Job
			if err := tx.HGetAll(ctx, jobKey).Scan(&job); err != nil {
				log.Printf("[ERROR] tx.HGetAll: %v", err)
			}
			if err := q.publisher(ctx, &job); err != nil {
				log.Printf("[ERROR] publisher(dequeue): %v", err)
			}

			log.Printf("[INFO] Job ID %s successfully dequeued for project %s and assigned to worker.\n", jobID, q.projectID)

			return nil
		}, projectKey, inProgressKey)
	}
	return retryIf(maxRetriesFromRedisTx, time.Second, isRetryable, f)
}

func (q *Queue) subscribeToKeySpaceNotifications(ctx context.Context) {
	pubsub := q.redis.PSubscribe(ctx, "__keyevent@0__:expired")
	defer pubsub.Close()

	for msg := range pubsub.Channel() {
		log.Println("[INFO] Received message on expiration event:", msg.Payload)
		expiredKey := msg.Payload

		switch {
		case strings.Contains(expiredKey, fmt.Sprintf("ack_deadline_in_project_%s", q.projectID)):
			jobID, err := extractIDs(expiredKey)
			if err != nil {
				log.Printf("[ERROR] Failed to extract IDs: %v", err)
				continue
			}
			if err := q.processTimeout(ctx, jobID, "ack deadline"); err != nil {
				log.Printf("[ERROR] processTimeout: %v", err)
				continue
			}

		case strings.Contains(expiredKey, fmt.Sprintf("execution_deadline_in_project_%s", q.projectID)):
			jobID, err := extractIDs(expiredKey)
			if err != nil {
				log.Printf("[ERROR] Failed to extract IDs: %v", err)
				continue
			}
			if err := q.processTimeout(ctx, jobID, "execution deadline"); err != nil {
				log.Printf("[ERROR] processTimeout: %v", err)
				continue
			}
		}
	}
}

func (q *Queue) processTimeout(ctx context.Context, jobID, reason string) error {
	jobKey := fmt.Sprintf(redisJobInfoKeyFormat, q.projectID, jobID)
	projectKey := fmt.Sprintf(redisQueueKeyFormat, q.projectID)
	inProgressKey := fmt.Sprintf(redisInProgressKeyFormat, q.projectID)

	status, err := q.redis.HGet(ctx, jobKey, "status").Result()
	if err != nil {
		return err
	}
	if status == statusAssigned || status == statusProcessing {
		err := q.redis.Watch(ctx, func(tx *redis.Tx) error {
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.HSet(ctx, jobKey, "status", statusSkipped, "skipped_at", time.Now(), "reason", reason)
				pipe.Decr(ctx, inProgressKey)
				return nil
			})
			if err == redis.Nil {
				return nil
			}
			if err != nil {
				return fmt.Errorf("processTimeout(TxPipelined): %w", err)
			}

			var job Job
			if err := tx.HGetAll(ctx, jobKey).Scan(&job); err != nil {
				log.Printf("[ERROR] tx.HGetAll: %v", err)
			}
			if err := q.publisher(ctx, &job); err != nil {
				log.Printf("[ERROR] publisher(processTimeout): %v", err)
			}

			log.Printf("[INFO] Job %s in this project %s has been skipped due to %s", jobID, q.projectID, reason)

			return nil
		}, jobKey, projectKey, inProgressKey)
		if err != nil {
			return fmt.Errorf("processTimeout(Watch): %w", err)
		}

		if err := q.dequeue(ctx); err != nil {
			return fmt.Errorf("processTimeout(dequeue): %w", err)
		}
		return nil
	}
	return nil
}

func (q *Queue) isEmpty(ctx context.Context) (bool, error) {
	len, err := q.redis.LLen(ctx, fmt.Sprintf(redisQueueKeyFormat, q.projectID)).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check queue length for project %s: %v", q.projectID, err)
	}

	return len == 0, nil
}

func (q *Queue) monitorQueueAndDequeue(ctx context.Context) {
	sleepDuration := time.Millisecond
	for {
		select {
		case <-ctx.Done():
			log.Printf("[INFO] Context canceled for project: %s", q.projectID)
			return

		default:
			if err := q.dequeue(ctx); err != nil {
				log.Printf("[ERROR] dequeue: %v", err)
			} else {
				sleepDuration = time.Second
			}

			// n, _ := q.redis.Get(ctx, fmt.Sprintf(redisInProgressKeyFormat, q.projectID)).Uint64()
			// log.Println("[INFO] Queue currently in progress:", n)

			isEmpty, err := q.isEmpty(ctx)
			if err != nil {
				log.Printf("[ERROR] isEmpty: %v", err)
				continue
			}
			if isEmpty {
				sleepDuration *= 2
				if sleepDuration > time.Minute {
					sleepDuration = time.Minute
				}
			}

			time.Sleep(sleepDuration)
		}
	}
}

func extractIDs(key string) (string, error) {
	parts := strings.Split(key, ":")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid key format: %s", key)
	}

	return parts[1], nil
}

func isRetryable(err error) bool {
	switch {
	case errors.Is(err, redis.TxFailedErr),
		errors.Is(err, ErrJobNotInQueue):
		return true

	default:
		return false
	}
}

func retryIf(attempts int, sleep time.Duration, isRetryable func(error) bool, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if !isRetryable(err) {
			return err
		}

		log.Printf("[RETRY] Attempt %d/%d failed: %v. Retrying in %v", i+1, attempts, err, sleep)
		time.Sleep(sleep)
		sleep = sleep * 2
	}

	log.Printf("[ERROR] Exceeded maximum retries (%d), last error: %v", attempts, err)
	return err
}
