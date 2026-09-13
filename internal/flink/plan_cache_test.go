package flink

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestPlanCacheExpiresUnusedJobsAndBoundsRecentEntries(t *testing.T) {
	requests := 0
	client := testClient(t, func(*http.Request) (string, int) {
		requests++
		return `{"plan":{"nodes":[]}}`, http.StatusOK
	})
	client.plans["expired"] = cachedPlan{fetchedAt: time.Now().Add(-2 * planCacheTTL)}
	for index := range planCacheMaxEntries + 5 {
		if _, err := client.loadPlan(context.Background(), fmt.Sprintf("job-%d", index), false); err != nil {
			t.Fatal(err)
		}
		if len(client.plans) > planCacheMaxEntries {
			t.Fatalf("cached %d plans after visiting %d jobs", len(client.plans), index+1)
		}
	}
	if _, retained := client.plans["expired"]; retained {
		t.Fatal("expired unused job plan is still retained")
	}
	if _, retained := client.plans["job-0"]; retained {
		t.Fatal("oldest plan was not evicted when the cache filled")
	}
	latest := fmt.Sprintf("job-%d", planCacheMaxEntries+4)
	if _, err := client.loadPlan(context.Background(), latest, false); err != nil {
		t.Fatal(err)
	}
	if requests != planCacheMaxEntries+5 {
		t.Fatal("freshly cached plan made another REST request")
	}
	client.plans[latest] = cachedPlan{fetchedAt: time.Now().Add(-2 * planCacheTTL)}
	if _, err := client.loadPlan(context.Background(), latest, false); err != nil {
		t.Fatal(err)
	}
	if requests != planCacheMaxEntries+6 {
		t.Fatal("expired plan was returned without refetching")
	}
	before := len(client.plans)
	if _, err := client.loadPlan(context.Background(), latest, true); err != nil {
		t.Fatal(err)
	}
	if requests != planCacheMaxEntries+7 || len(client.plans) != before {
		t.Fatal("forced refresh did not replace just the requested cache entry")
	}
}
