package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// AdClickEvent represents a raw high-volume payload ingested from ad channels.
type AdClickEvent struct {
	CampaignID string
	Cost       float64
	ClickedAt  time.Time
}

// CampaignStats stores our aggregated analytical metrics in memory.
type CampaignStats struct {
	TotalClicks int
	TotalSpend  float64
}

// Pipeline orchestrates memory safety and streaming components.
type Pipeline struct {
	eventStream chan AdClickEvent
	metrics     map[string]*CampaignStats
	mu          sync.Mutex // Protects the metrics map from concurrent write panic/race conditions
}

func main() {
	// Initialize pipeline with a buffered channel (our mock Kafka topic)
	p := &Pipeline{
		eventStream: make(chan AdClickEvent, 500), // room for 500 events, lets producer send without blocking
		metrics:     make(map[string]*CampaignStats),
	}

	// 1. Spawn the background consumer worker loop
	go p.startConsumerWorker()

	// 2. Spawn the periodic batch flusher (runs every 2 seconds)
	go p.startBatchFlusher(2 * time.Second)

	// 3. Kick off mock traffic generation
	fmt.Println("[System] Adtech ingestion pipeline online. Streaming incoming clicks...")
	p.generateMockTraffic()
}

// startConsumerWorker reads incoming raw events from the channel asynchronously
func (p *Pipeline) startConsumerWorker() {
	for event := range p.eventStream {
		// Mutex locking prevents race conditions on the shared metrics map
		//p.mu.Lock()

		// Initialize state for new campaigns
		if _, exists := p.metrics[event.CampaignID]; !exists {
			p.metrics[event.CampaignID] = &CampaignStats{}
		}

		// In-memory aggregation
		p.metrics[event.CampaignID].TotalClicks++
		p.metrics[event.CampaignID].TotalSpend += event.Cost

		//p.mu.Unlock()
	}
}

// startBatchFlusher periodically clears memory and logs data (simulating an OLAP database flush)
func (p *Pipeline) startBatchFlusher(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		//p.mu.Lock()

		if len(p.metrics) == 0 {
			fmt.Println("\n--- [OLAP Flush] No events captured in this window ---")
			//p.mu.Unlock()
			continue
		}

		fmt.Println("\n==== [OLAP Batch Flush] Writing metrics to Analytics DB ====")
		for campaignID, stats := range p.metrics {
			fmt.Printf("Campaign [%s] -> Clicks: %d | Total Spend: $%.2f\n", 
				campaignID, stats.TotalClicks, stats.TotalSpend)
		}
		fmt.Println("============================================================")

		// Clear local state after a successful database save to prepare for the next batch
		p.metrics = make(map[string]*CampaignStats)

		//p.mu.Unlock()
	}
}

// generateMockTraffic fires a continuous stream of random events into our channel
func (p *Pipeline) generateMockTraffic() {
	campaigns := []string{"CAMP_BLACK_FRIDAY", "CAMP_SUMMER_SALE", "CAMP_RE_ENGAGEMENT"}

	// Simulate 100 fast-paced events
	for i := 0; i < 100; i++ {
		event := AdClickEvent{
			CampaignID: campaigns[rand.Intn(len(campaigns))],
			Cost:       0.05 + rand.Float64()*(1.50-0.05), // Cost per click ranges between $0.05 and $1.50
			ClickedAt:  time.Now(),
		}

		// Non-blocking fire-and-forget push to our channel
		p.eventStream <- event

		// Slight throttle to simulate network spacing
		time.Sleep(40 * time.Millisecond)
	}

	// Give the flusher loop one final moment to catch remaining metrics before exiting
	time.Sleep(3 * time.Second)
	fmt.Println("[System] Code execution finished.")
}