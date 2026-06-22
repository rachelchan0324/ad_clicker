package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	_ "modernc.org/sqlite" // Blank import registers the SQLite driver
)

type AdClickEvent struct {
	CampaignID string
	Cost       float64
	ClickedAt  time.Time
}

type CampaignStats struct {
	TotalClicks int
	TotalSpend  float64
}

type Pipeline struct {
	eventStream chan AdClickEvent
	metrics     map[string]*CampaignStats
	mu          sync.Mutex
	db          *sql.DB // Reference to our active SQL database connection pool
}

func main() {
	// 1. Initialize the local SQLite database file
	db, err := sql.Open("sqlite", "./campaign_analytics.db")
	if err != nil {
		log.Fatalf("[DB Error] Failed to open database: %v", err)
	}
	defer db.Close()

	// 2. Create the analytics tracking table if it doesn't exist yet
	createTableSQL := `CREATE TABLE IF NOT EXISTS campaign_metrics (
		campaign_id TEXT PRIMARY KEY,
		total_clicks INTEGER DEFAULT 0,
		total_spend REAL DEFAULT 0.0
	);`
	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatalf("[DB Error] Failed to initialize table: %v", err)
	}
	fmt.Println("[System] Database table 'campaign_metrics' initialized successfully.")

	// 3. Initialize pipeline with the database handle
	p := &Pipeline{
		eventStream: make(chan AdClickEvent, 500),
		metrics:     make(map[string]*CampaignStats),
		db:          db,
	}

	go p.startConsumerWorker()
	go p.startBatchFlusher(2 * time.Second)

	fmt.Println("[System] Adtech ingestion pipeline online. Streaming incoming clicks...")
	p.generateMockTraffic()
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

// startBatchFlusher handles database transactions to persist metrics
func (p *Pipeline) startBatchFlusher(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		p.mu.Lock()

		if len(p.metrics) == 0 {
			p.mu.Unlock()
			continue
		}

		fmt.Println("\n==== [OLAP Batch Flush] Writing metrics to SQLite Database ====")

		// Start an explicit SQL Transaction (Atomic operation)
		tx, err := p.db.Begin()
		if err != nil {
			log.Printf("[DB Error] Could not begin transaction: %v", err)
			p.mu.Unlock()
			continue
		}

		// Prepare an Upsert statement (Insert if new, accumulate values if campaign exists)
		upsertSQL := `INSERT INTO campaign_metrics (campaign_id, total_clicks, total_spend) 
			VALUES (?, ?, ?)
			ON CONFLICT(campaign_id) DO UPDATE SET 
				total_clicks = total_clicks + excluded.total_clicks,
				total_spend = total_spend + excluded.total_spend;`

		stmt, err := tx.Prepare(upsertSQL)
		if err != nil {
			log.Printf("[DB Error] Failed to prepare statement: %v", err)
			tx.Rollback()
			p.mu.Unlock()
			continue
		}

		// Loop through our local batch cache and execute queries via the prepared statement
		for campaignID, stats := range p.metrics {
			_, err := stmt.Exec(campaignID, stats.TotalClicks, stats.TotalSpend)
			if err != nil {
				log.Printf("[DB Error] Exec failing for %s: %v", campaignID, err)
			} else {
				fmt.Printf("Successfully flushed batch metrics for %s to disk.\n", campaignID)
			}
		}

		// Commit the transaction to write changes to disk permanently
		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Printf("[DB Error] Transaction commit failed: %v", err)
		} else {
			fmt.Println("================================================================")
			// Clear local state only after a successful database commit
			p.metrics = make(map[string]*CampaignStats)
		}

		p.mu.Unlock()
	}
}

func (p *Pipeline) generateMockTraffic() {
	campaigns := []string{"CAMP_BLACK_FRIDAY", "CAMP_SUMMER_SALE", "CAMP_RE_ENGAGEMENT"}

	for i := 0; i < 100; i++ {
		event := AdClickEvent{
			CampaignID: campaigns[rand.Intn(len(campaigns))],
			Cost:       0.05 + rand.Float64()*(1.50-0.05),
			ClickedAt:  time.Now(),
		}
		p.eventStream <- event
		time.Sleep(40 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)
	fmt.Println("[System] Ingestion pipeline run finished.")
}