# Simple Job Queue with Redis

A simple Go library for creating a job queue with Redis as its backend.

## Features

* Supports concurrent job processing
* Configurable maximum execution time and acknowledgement deadline
* Uses Redis for storing and retrieving jobs

## Usage
Create a new Queue instance and use its methods to ```Join```:

```golang
    q, err := queue.New(ctx, &queue.Config{
      Redis:            redisClient,
      MaxWorker:        2,
      AckDeadline:      60,
      MaxExecutionTime: 3000,
      ProjectID:        "PROJECT_ID",
      BatchSize:        20,
      Backup: func(ctx context.Context, data []queue.JobBackup) error {
        d, _ := json.Marshal(data)
        log.Printf("[INFO] Backup: %s", d)
        return nil
      },
      Publisher: func(ctx context.Context, job *queue.Job) error {
        d, _ := json.Marshal(job)
        log.Printf("[INFO] PublishJob: %s", d)
        return nil
      },
    })
    if err != nil {
      log.Fatalf("queue.NewWithDefault: %v", err)
    }

    // To join the queue
    job, err := q.Join(context.Background(), "JOB_1")
    if err != nil {
      log.Fatalf("q.Join: %v", err)
    }
  
    log.Printf("%+v", job)
```

## Configuration
The Queue struct has the following fields:
  -  **Redis**: a Redis client instance
  -  **ProjectID**: the ID of the project that the queue belongs to
  -  **MaxWorker**: the maximum number of jobs that can be processed in parallel (default: 5)
  -  **AckDeadline**: the number of seconds a job can wait to be acknowledged after being queued (default: 60)
  -  **MaxExecutionTime**: the maximum duration (in seconds) allowed for a job or process to complete (default: no time limit)
  -  **BatchSize**: the maximum number of rows to process in backup mode (default: no backup mode)
  -  **Secret**: the secret used to make the signature of the message (**NOT DONE**)
  -  **Backup**: the function used to backup the queue when cleaning up (default: does nothing)
  -  **Publisher**: the function used to publish the job when dequeueing (default: does nothing)

## Contributing
Contributions are welcome! Please submit a pull request with your changes.

## License
This library is licensed under the MIT License.
