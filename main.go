package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	steamstore "github.com/ShadowDash2000/steam-store-go"
	"resty.dev/v3"
)

type Scraper struct {
	ctx        context.Context
	client     *steamstore.Client
	f          *os.File
	outName    string
	wroteFirst bool
	wroteCount int64
}

func NewScraper(ctx context.Context, outName string, f *os.File) *Scraper {
	c := steamstore.New()
	c.Client().
		SetRetryCount(10).
		SetRetryWaitTime(1 * time.Second).
		SetRetryMaxWaitTime(10 * time.Second).
		DisableRetryDefaultConditions().
		AddRetryConditions(func(res *resty.Response, err error) bool {
			if res.StatusCode() == 500 {
				return false
			}
			if res.StatusCode() < 200 || res.StatusCode() >= 400 {
				return true
			}
			return false
		})

	return &Scraper{
		ctx:     ctx,
		client:  c,
		f:       f,
		outName: outName,
	}
}

func (s *Scraper) writeHeader() {
	header := struct {
		SchemaVersion int    `json:"schema_version"`
		ScrapedAt     string `json:"scraped_at"`
		Source        string `json:"source"`
	}{
		SchemaVersion: 1,
		ScrapedAt:     time.Now().UTC().Format(time.RFC3339),
		Source:        "steam-spy",
	}
	encHeader, _ := json.Marshal(header)
	if _, err := s.f.Write(encHeader[:len(encHeader)-1]); err != nil {
		log.Fatalf("write header: %v", err)
	}
	if _, err := s.f.WriteString(",\"data\":["); err != nil {
		log.Fatalf("write data open bracket: %v", err)
	}
}

func (s *Scraper) writeFooter() {
	if _, err := s.f.WriteString("]}"); err != nil {
		log.Fatalf("write footer: %v", err)
	}
}

func (s *Scraper) writeItemBytes(b []byte) {
	if !s.wroteFirst {
		if _, err := s.f.Write(b); err != nil {
			log.Printf("Write first item error: %v", err)
			return
		}
		s.wroteFirst = true
	} else {
		if _, err := s.f.WriteString(","); err != nil {
			log.Printf("Write comma error: %v", err)
			return
		}
		if _, err := s.f.Write(b); err != nil {
			log.Printf("Write item error: %v", err)
			return
		}
	}
	s.wroteCount++
}

func (s *Scraper) processAllPages() {
	page := uint(1)
	for {
		res, err := s.client.GetSteamSpyAppsPaginated(s.ctx, page)
		if err != nil {
			if errors.Is(err, steamstore.ErrSteamSpyLastPage) {
				log.Printf("Reached last page at %d", page)
				return
			}
			log.Printf("Fetch page %d error: %v", page, err)
			return
		}
		for _, app := range res {
			b, err := json.Marshal(app)
			if err != nil {
				log.Printf("Marshal app error: %v", err)
				continue
			}
			s.writeItemBytes(b)
		}
		log.Printf("Page %d scraped", page)
		page++
	}
}

func main() {
	outName := fmt.Sprintf("steam-spy-%s.json", time.Now().UTC().Format("20060102"))
	f, err := os.Create(outName)
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer f.Close()

	s := NewScraper(context.Background(), outName, f)
	s.writeHeader()
	s.processAllPages()
	s.writeFooter()

	log.Printf("Scraping completed. Result written to %s", s.outName)
	log.Printf("Actually written=%d", s.wroteCount)
}
