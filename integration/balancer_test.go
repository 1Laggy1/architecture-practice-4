
package integration

import (
 "fmt"
 "net/http"
 "os"
 "testing"
 "time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
 Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
 if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
  t.Skip("Integration test is not enabled")
 }

 resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
 if err != nil {
  t.Fatalf("Failed to send request to balancer: %v", err)
 }
 defer resp.Body.Close()

 if resp.StatusCode != http.StatusOK {
  t.Fatalf("Expected status OK but got %v", resp.Status)
 }

 lbFrom := resp.Header.Get("lb-from")
 if lbFrom == "" {
  t.Errorf("Expected 'lb-from' header but got none")
 }
 t.Logf("Response from [%s]", lbFrom)
}

func BenchmarkBalancer(b *testing.B) {
 if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
  b.Skip("Integration test is not enabled")
 }

 b.ResetTimer()
 for i := 0; i < b.N; i++ {
  resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
  if err != nil {
   b.Fatalf("Failed to send request to balancer: %v", err)
  }
  resp.Body.Close()

  if resp.StatusCode != http.StatusOK {
   b.Fatalf("Expected status OK but got %v", resp.Status)
  }
 }
}

