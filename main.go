package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	_ "modernc.org/sqlite" // Pure Go SQLite driver
)

// AdClickEvent handles the incoming JSON API payload schema
type AdClickEvent struct {
	CampaignID string    `json:"campaign_id"`
	Cost       float64   `json:"cost"`
	ClickedAt  time.Time `json:"clicked_at"`
}

type CampaignStats struct {
	TotalClicks int
	TotalSpend  float64
}

type Pipeline struct {
	eventStream chan AdClickEvent
	metrics     map[string]*CampaignStats
	mu          sync.Mutex
	db          *sql.DB
}

func main() {
	// 1. Initialize local SQLite file-backed database
	db, err := sql.Open("sqlite", "./campaign_analytics.db")
	if err != nil {
		log.Fatalf("[DB Error] Failed to open database: %v", err)
	}
	defer db.Close()

	// 2. Setup database table infrastructure
	createTableSQL := `CREATE TABLE IF NOT EXISTS campaign_metrics (
		campaign_id TEXT PRIMARY KEY,
		total_clicks INTEGER DEFAULT 0,
		total_spend REAL DEFAULT 0.0
	);`
	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatalf("[DB Error] Failed to initialize table: %v", err)
	}
	fmt.Println("[System] Database table 'campaign_metrics' verified.")

	// 3. Initialize pipeline infrastructure
	p := &Pipeline{
		eventStream: make(chan AdClickEvent, 1000), // Buffered channel
		metrics:     make(map[string]*CampaignStats),
		db:          db,
	}

	// Spin up our asynchronous worker loops
	go p.startConsumerWorker()
	go p.startBatchFlusher(3 * time.Second)

	// 4. Register HTTP route handlers
	http.HandleFunc("/clicks", p.handleClickRequest) // when someone hits path/clicks, run handleClickRequest function

	// Start the active HTTP Server on local port 8080
	port := ":8080"
	fmt.Printf("[System] Ingestion Server running locally on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("[System Error] HTTP server failed to launch: %v", err)
	}
}

// handleClickRequest processes incoming web requests and drops them into the stream
func (p *Pipeline) handleClickRequest(w http.ResponseWriter, r *http.Request) {
	// Enforce POST requests exclusively
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	// Decode JSON payload into an AdClickEvent struct
	var event AdClickEvent
	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		http.Error(w, "Bad request: Invalid JSON payload structure.", http.StatusBadRequest)
		return
	}

	// Populate timestamp context if missing
	if event.ClickedAt.IsZero() {
		event.ClickedAt = time.Now()
	}

	// Fire-and-forget: Push directly into our async stream pipeline channel 
	p.eventStream <- event

	// Send an immediate 202 Accepted status code to the client
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"event_queued_successfully"}`))
}

func (p *Pipeline) startConsumerWorker() {
	for event := range p.eventStream {
		p.mu.Lock()
		if _, exists := p.metrics[event.CampaignID]; !exists {
			p.metrics[event.CampaignID] = &CampaignStats{}
		}
		p.metrics[event.CampaignID].TotalClicks++
		p.metrics[event.CampaignID].TotalSpend += event.Cost
		p.mu.Unlock()
	}
}

func (p *Pipeline) startBatchFlusher(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		p.mu.Lock()
		if len(p.metrics) == 0 {
			p.mu.Unlock()
			continue
		}

		fmt.Println("\n==== [OLAP Batch Flush] Committing metrics to SQL DB ====")
		tx, err := p.db.Begin()
		if err != nil {
			log.Printf("[DB Error] Transaction begin error: %v", err)
			p.mu.Unlock()
			continue
		}

		upsertSQL := `INSERT INTO campaign_metrics (campaign_id, total_clicks, total_spend) 
			VALUES (?, ?, ?)
			ON CONFLICT(campaign_id) DO UPDATE SET 
				total_clicks = total_clicks + excluded.total_clicks,
				total_spend = total_spend + excluded.total_spend;`

		stmt, err := tx.Prepare(upsertSQL)
		if err != nil {
			log.Printf("[DB Error] Statement prepare failed: %v", err)
			tx.Rollback()
			p.mu.Unlock()
			continue
		}

		for campaignID, stats := range p.metrics {
			_, err := stmt.Exec(campaignID, stats.TotalClicks, stats.TotalSpend)
			if err != nil {
				log.Printf("[DB Error] Exec failed for %s: %v", campaignID, err)
			} else {
				fmt.Printf("Flushed metrics batch: %s\n", campaignID)
			}
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Printf("[DB Error] Commit phase failure: %v", err)
		} else {
			fmt.Println("=========================================================")
			p.metrics = make(map[string]*CampaignStats) // Safely clear batch cache
		}
		p.mu.Unlock()
	}
}