package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	logFileMu sync.Mutex
	logPath   = getEnv("LOG_PATH", "/app/logs/csv_logs.log")
)

func main() {
	port := getEnv("PORT", "8080")

	http.HandleFunc("/upload", handleUpload)
	http.HandleFunc("/health", handleHealth)

	log.Printf("CSV Upload API started on port %s\n", port)
	log.Printf("Log output path: %s\n", logPath)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// handleHealth returns a simple health check response
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

// handleUpload receives a CSV file and writes each row as a JSON log line
func handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed. Use POST."}`, http.StatusMethodNotAllowed)
		return
	}

	// Parse multipart form (max 10MB)
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Failed to parse form: %v"}`, err), http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Failed to get file: %v"}`, err), http.StatusBadRequest)
		return
	}
	defer file.Close()

	log.Printf("Received file: %s (%d bytes)\n", header.Filename, header.Size)

	// Parse CSV
	reader := csv.NewReader(file)

	// Read header row
	csvHeader, err := reader.Read()
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"Failed to read CSV header: %v"}`, err), http.StatusBadRequest)
		return
	}

	// Trim spaces from headers
	for i := range csvHeader {
		csvHeader[i] = strings.TrimSpace(csvHeader[i])
	}

	// Read all rows and write JSON logs
	var rowCount int
	var errors []string

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errors = append(errors, fmt.Sprintf("row %d: %v", rowCount+2, err))
			continue
		}

		// Map row to JSON object
		record := make(map[string]interface{})
		for i, val := range row {
			if i < len(csvHeader) {
				// Try to parse as float/int, fallback to string
				trimVal := strings.TrimSpace(val)
				if intVal, err := strconv.Atoi(trimVal); err == nil {
					record[csvHeader[i]] = intVal
				} else if floatVal, err := strconv.ParseFloat(trimVal, 64); err == nil {
					record[csvHeader[i]] = floatVal
				} else {
					record[csvHeader[i]] = trimVal
				}
			}
		}

		if err := writeLogLine(record); err != nil {
			errors = append(errors, fmt.Sprintf("row %d: write error: %v", rowCount+2, err))
			continue
		}

		rowCount++
	}

	// Response
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]interface{}{
		"message":    fmt.Sprintf("Processed %d rows successfully", rowCount),
		"rows":       rowCount,
		"timestamp":  time.Now().Format(time.RFC3339),
		"log_output": logPath,
	}
	if len(errors) > 0 {
		resp["errors"] = errors
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)

	log.Printf("Processed %d rows from %s, %d errors\n", rowCount, header.Filename, len(errors))
}

func writeLogLine(record map[string]interface{}) error {
	jsonData, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	logFileMu.Lock()
	defer logFileMu.Unlock()

	// Ensure directory exists
	dir := logPath[:strings.LastIndex(logPath, "/")]
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	_, err = f.Write(jsonData)
	if err != nil {
		return err
	}
	_, err = f.Write([]byte("\n"))
	return err
}
