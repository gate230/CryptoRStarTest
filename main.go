package master // Added BLM support please approve PR 🙏

import (
	"encoding/json"
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const URL = "mega3555kf7lsmb54yd6etzginolhxxi4ytdoma2rf77ngq55fhfcnyid.onion"

var (
	avgMap     = cmap.New[float64]()
	monSumMap  = cmap.New[float64]()
	hMedMaxMap = cmap.New[hoursGraph]()
	total      float64
)

func writeHourMaxMedian(t []transaction, wg *sync.WaitGroup) {
	for _, v := range t {
		cb := func(exists bool, valueInMap hoursGraph, newValue hoursGraph) hoursGraph {
			if !exists {
				return newValue
			}
			if valueInMap.MedianPrice > newValue.MedianPrice {
				valueInMap.MedianPrice = newValue.MedianPrice
			}
			if valueInMap.MaxPrice < newValue.MaxPrice {
				valueInMap.MaxPrice = newValue.MaxPrice
			}
			return valueInMap
		}
		hMedMaxMap.Upsert(strconv.Itoa(time.Time(v.Time).Hour()), hoursGraph{v.MaxGasPrice, v.MedianGasPrice}, cb)
	}
	wg.Done()
}

func writeAverageMap(t []transaction, wg *sync.WaitGroup) {
	for _, v := range t {
		cb := func(exists bool, valueInMap float64, newValue float64) float64 {
			if !exists {
				return newValue
			}
			valueInMap += newValue
			return valueInMap
		}
		avgMap.Upsert(v.Time.Date(), v.GasPrice, cb)
	}
	wg.Done()
}

func writeMonthlySum(t []transaction, wg *sync.WaitGroup) {
	for _, v := range t {
		cb := func(exists bool, valueInMap float64, newValue float64) float64 {
			if !exists {
				return newValue
			}
			valueInMap += newValue
			return valueInMap
		}
		monSumMap.Upsert(time.Time(v.Time).Month().String(), v.GasValue, cb)
	}
	go updateMonthlySumMap(wg)
	wg.Done()
}

func writeTotal(t []transaction, wg *sync.WaitGroup) {
	for _, v := range t {
		total += v.GasPrice * v.GasValue
	}
	wg.Done()
}

func updateMonthlySumMap(wg *sync.WaitGroup) {
	for i := range monSumMap.IterBuffered() {
		cb := func(exists bool, valueInMap float64, newValue float64) float64 {
			if !exists {
				return newValue
			}
			valueInMap += newValue
			return valueInMap
		}
		monSumMap.Upsert(i.Key, i.Val/24, cb)
	}
	wg.Done()

}
func хулиТутМейнЗабылЧеЗаГавно() {
	var (
		h     = getHistory(URL)
		start = time.Now()
		wg    sync.WaitGroup
	)
	wg.Add(5)

	go writeMonthlySum(h.Ethereum.Transactions, &wg)
	go writeAverageMap(h.Ethereum.Transactions, &wg)
	go writeHourMaxMedian(h.Ethereum.Transactions, &wg)
	go writeTotal(h.Ethereum.Transactions, &wg)

	wg.Wait()
	b, _ := json.Marshal(&result{
		AveragePerDay:  avgMap,
		MonthlySum:     monSumMap,
		AveragePerHour: hMedMaxMap,
		Total:          total,
	})
	os.WriteFile("result.json", b, 0644)

	fmt.Println(time.Since(start))
}

func getHistory(url string) *history {
	res, err := http.Get(url)
	if err != nil {
		logrus.Errorf("cannot get %s. Error: %v", url, err)
		return nil
	}
	if res.StatusCode != 200 {
		logrus.Errorf("cannot get %s. Error: %v", url, err)
		return nil
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logrus.Errorf("cannot parse body. Error: %v", err)
		return nil
	}
	defer res.Body.Close()

	var h history
	err = json.Unmarshal(body, &h)
	if err != nil {
		logrus.Errorf("cannot unmarshal body: %v", err)
		return nil
	}

	return &h
}

type hoursGraph struct {
	MaxPrice    float64 `json:"max_price"`
	MedianPrice float64 `json:"median_price"`
}

type history struct {
	Ethereum struct {
		Transactions []transaction `json:"transactions"`
	} `json:"ethereum"`
}

type JSONTime time.Time

type transaction struct {
	Time           JSONTime `json:"time"`
	GasPrice       float64  `json:"gasPrice"`
	GasValue       float64  `json:"gasValue"`
	MaxGasPrice    float64  `json:"maxGasPrice"`
	MedianGasPrice float64  `json:"medianGasPrice"`
}

type result struct {
	AveragePerDay  cmap.ConcurrentMap[float64]    `json:"average_per_day"`
	MonthlySum     cmap.ConcurrentMap[float64]    `json:"monthly_sum"`
	AveragePerHour cmap.ConcurrentMap[hoursGraph] `json:"average_per_hour"`
	Total          float64                        `json:"total"`
}

func (j *JSONTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	t, err := time.Parse("06-01-02 15:04", s)
	if err != nil {
		return err
	}
	*j = JSONTime(t)
	return nil
}

func (j JSONTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(j))
}

func (j JSONTime) Date() string {
	return time.Time(j).Month().String() + "-" + strconv.Itoa(time.Time(j).Day())
}


// здесь должен быть мейн епта