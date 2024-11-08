package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"text/template"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/p40pmn/redis-queue/queue"
	"github.com/redis/go-redis/v9"
)

//go:embed templates/*.html
var templatesFS embed.FS // Embed all files under /templates into templatesFS

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func main() {
	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", getEnv("REDIS_HOST", "127.0.0.1"), getEnv("REDIS_PORT", "6379")),
		MinIdleConns: 10,
		PoolSize:     15,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis client not connected: %v", err)
	}
	defer redisClient.Close()

	registry, err := queue.NewRegistry(&queue.RegistryConfig{
		Redis:     redisClient,
		BatchSize: 20,
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
		log.Fatalf("queue.NewRegistry: %v", err)
	}
	defer registry.Close(ctx)

	q, err := registry.GetOrCreateQueue(ctx, &queue.Config{
		MaxWorker:        2,
		AckDeadline:      5,
		MaxExecutionTime: 30,
		ProjectID:        getEnv("PROJECT_ID", "LAOITDEV"),
	})
	if err != nil {
		log.Fatalf("registry.GetOrCreateQueue: %v", err)
	}

	h := &handle{
		q: q,
	}

	e := echo.New()

	renderer := &TemplateRenderer{
		templates: template.Must(template.ParseFS(templatesFS, "templates/*.html")),
	}
	e.Renderer = renderer
	e.GET("/ws/:jobID", h.wsHandler)
	e.GET("/", h.homeHandler)
	e.GET("/ack/:jobID", h.ackHandler)
	e.GET("/done/:jobID", h.doneHandler)

	e.POST("/join", h.joinHandler)

	e.HideBanner = true

	errCh := make(chan error, 1)
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, os.Kill)
	defer cancel()

	go func() {
		errCh <- e.Start(fmt.Sprintf(":%s", getEnv("PORT", "3300")))
	}()

	select {
	case <-ctx.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := e.Shutdown(ctx); err != nil {
			log.Fatalf("shutdown server failure: %v", err)
		}
		log.Println("server shutdown")

	case err := <-errCh:
		log.Fatalf("start server error +: %v", err)
	}
}

type TemplateRenderer struct {
	templates *template.Template
}

func (tr *TemplateRenderer) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return tr.templates.ExecuteTemplate(w, name, data)
}

type handle struct {
	q *queue.Queue
}

type Jop struct {
	Name        string
	QueueNumber uint64
	ID          string
	ProjectID   string
	Status      string
	JoinedAt    time.Time
}

func (H *handle) ackHandler(c echo.Context) error {
	jobID := c.Param("jobID")
	if err := H.q.Ack(context.Background(), jobID); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, echo.Map{"status": "Success"})
}

func (h *handle) doneHandler(c echo.Context) error {
	jobID := c.Param("jobID")
	if err := h.q.Done(context.Background(), jobID); err != nil {
		return err
	}

	if err := c.Render(http.StatusOK, "done.html", nil); err != nil {
		log.Printf("c.Render: %v", err)
		return err
	}
	return nil
}

func (h *handle) homeHandler(c echo.Context) error {
	return c.Render(http.StatusOK, "index.html", nil)
}

var qNumber = 0

func (h *handle) joinHandler(c echo.Context) error {
	name := c.FormValue("name")
	qNumber++
	id := fmt.Sprintf("JOB_%d", qNumber)

	a, err := h.q.Join(context.Background(), id)
	if err != nil {
		log.Printf("q.Join: %v", err)
		return err
	}

	var job Jop
	job.Name = name
	job.QueueNumber = a.QueueNumber
	job.ID = id
	job.ProjectID = a.ProjectID
	job.Status = a.Status
	job.JoinedAt = a.JoinedAt
	if err := c.Render(http.StatusOK, "join.html", job); err != nil {
		log.Printf("c.Render: %v", err)
		return err
	}
	return nil
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var wsClientConnectionsMap = make(map[string]*websocket.Conn)
var wsClientMutex = sync.Mutex{}

func (h *handle) wsHandler(c echo.Context) error {
	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return err
	}
	defer ws.Close()

	jobID := c.Param("jobID")
	ctx := c.Request().Context()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			queueLength, err := h.q.GetLength(ctx)
			if err != nil {
				c.Logger().Error("Error fetching queue length:", err)
				return err
			}

			job, err := h.q.GetJob(ctx, jobID)
			if err != nil {
				c.Logger().Error("Error fetching job:", err)
				return err
			}

			if job.Status != "queued" {
				c.Logger().Info("Job is done, closing WebSocket connection")
				err = ws.WriteJSON(map[string]interface{}{
					"queue_size":      0,
					"last_updated_at": time.Now().Format("02 Jan 2006 15:04:05"),
					"total_jobs":      queueLength,
					"status":          job.Status,
					"job_id":          job.ID,
				})
				if err != nil {
					c.Logger().Error("Error sending final message:", err)
					return err
				}

				err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Job completed"))
				if err != nil {
					c.Logger().Error("Error sending close message:", err)
					return err
				}

				ws.Close()
				return nil
			}

			position, err := h.q.GetPosition(ctx, jobID)
			if err != nil {
				c.Logger().Info("Error fetching position:", err)
				// return err
			}
			err = ws.WriteJSON(map[string]interface{}{
				"queue_size":      position,
				"last_updated_at": time.Now().Format("02 Jan 2006 15:04:05"),
				"total_jobs":      queueLength,
				"status":          job.Status,
				"job_id":          job.ID,
			})
			if err != nil {
				c.Logger().Error("WebSocket write error:", err)
				return err
			}
		}
	}
}
