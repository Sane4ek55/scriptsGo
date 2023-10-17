package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
	"unicode"
)

const (
	concurrentRequests = 1  //number of Goroutines
	batchSize          = 1  //size batch
	endpoint           = "" //url handler
	fileUrlInput       = "" //path to the file with input data
	fileUrlOut         = "" //path to the file with output data
)

type Body struct {
	IDs  []int64  `json:"ids"`
	Data []string `json:"data"`
}

func main() {
	start := time.Now()
	for i := 1; i <= 1; i++ {
		strI := strconv.Itoa(i)
		varinant_ids, err := readInputFile(fileUrlInput + strI)
		if err != nil {
			log.Fatalln("Scanner error:", err)
		}

		var wg sync.WaitGroup
		ch := make(chan []int64, concurrentRequests)

		for i := 0; i < concurrentRequests; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for batch := range ch {
					sendBatch(batch, strI)
				}
			}()
		}

		for i := 0; i < len(varinant_ids); i += batchSize {
			end := i + batchSize
			if end > len(varinant_ids) {
				end = len(varinant_ids)
			}

			ch <- varinant_ids[i:end]
		}

		close(ch)
		wg.Wait()
	}

	duration := time.Since(start)
	fmt.Println(duration)
}

func sendBatch(batch []int64, strI string) {

	data, err := json.Marshal(Body{
		IDs:  batch,
		Data: []string{"testData"},
	})
	if err != nil {
		fmt.Println("Error marshaling data:", err)
		return
	}

	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(data))
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	req.Header.Add("User-Name", "testUser")
	resp, _ := http.DefaultClient.Do(req)

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Received non-OK response:", resp.Status)
	}

	if resp.StatusCode == http.StatusForbidden {
		fmt.Println("Received non-OK response:", resp.Status)
		os.Exit(1)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}

	var jsonMap map[string][]interface{}
	err_unmarshal := json.Unmarshal([]byte(string(body)), &jsonMap)
	if err_unmarshal != nil {
		fmt.Println(err)
		return
	}
	for _, val := range jsonMap["test"] {
		strValue1 := fmt.Sprintf("%v", val.(map[string]interface{})["test"])
		if strValue1 == "[]" {
			fmt.Println("No data!")
			continue
		}
		data := val.(map[string]interface{})["tests"].([]interface{})[0].(map[string]interface{})["tests"].([]interface{})[0].(map[string]interface{})["tests"]
		strData := fmt.Sprintf("%v", data)
		if !isDigital(strData) {
			testID := int(val.(map[string]interface{})["test"].(float64))
			strTestID := strconv.Itoa(testID)
			dataID := val.(map[string]interface{})["test"].([]interface{})[0].(map[string]interface{})["test"]
			strDataID := fmt.Sprintf("%v", dataID)
			writeFile(strTestID, strDataID, strData)
		}

	}

	fmt.Println(resp.Status, batch[len(batch)-1], strI)
}

func isDigital(s string) bool {
	for _, word := range s {
		if !unicode.IsDigit(word) {
			return false
		}
	}
	return true
}

func readInputFile(fileUrl string) ([]int64, error) {
	inputData := []int64{}

	file, err := os.Open(fileUrl)
	if err != nil {
		log.Fatalf("Open is error: %s", err)
		os.Exit(1)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()

		i, err := strconv.Atoi(text)
		if err != nil {
			continue
		}
		inputData = append(inputData, int64(i))
	}

	return inputData, scanner.Err()

}

func writeFile(variantID string, strAttributeID string, strValue string) {

	w, err := NewCsvWriter(fileUrlOut)
	if err != nil {
		log.Panic(err)
	}

	w.Write([]string{variantID, strAttributeID, strValue})

	w.Flush()
}

type CsvWriter struct {
	mutex     *sync.Mutex
	csvWriter *csv.Writer
}

func NewCsvWriter(fileName string) (*CsvWriter, error) {
	csvFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)

	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(csvFile)
	return &CsvWriter{csvWriter: w, mutex: &sync.Mutex{}}, nil
}

func (w *CsvWriter) Write(row []string) {
	w.mutex.Lock()
	w.csvWriter.Write(row)
	w.mutex.Unlock()
}

func (w *CsvWriter) Flush() {
	w.mutex.Lock()
	w.csvWriter.Flush()
	w.mutex.Unlock()
}
