package main

import (
	"appsinstalled"
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/rainycape/memcache"
)

// Структура, в которой храниться распаршенная запись из файла логов
type AppsInstalled struct {
	DeviceType string
	DeviceID   string
	Lat        float64
	Lon        float64
	Apps       []uint32
}

type ProtobufMes struct {
	Key   string
	Value *[]byte
}

type counters struct {
	all int
	err int
}

func dotRename(path string) {
	head, tail := filepath.Split(path)
	dotNamePath := filepath.Join(head, "."+tail)
	err := os.Rename(path, dotNamePath)
	if err != nil {
		log.Print(err)
	} else {
		log.Printf("File renamed to %s", dotNamePath)
	}

}

func stats(errors chan counters, threshold float64) {
	allCounts := 0
	errCounts := 0
	for counts := range errors {
		allCounts += counts.all
		errCounts += counts.err
	}
	errorsRatio := float64(errCounts) / float64(allCounts)
	if errorsRatio > threshold {
		log.Printf("Many errors occurred: %f > %f.", errorsRatio, threshold)
	}

}

// создание задач
func dispatch(files []string, devices *map[string]*string,
	attempts int, dry bool, timeOut int, threshold float64) {
	var wg sync.WaitGroup
	quit := make(chan bool)
	errors := make(chan counters)

	messages := make(map[string]chan *ProtobufMes, len(*devices))
	for devType, addr := range *devices {
		client, err := memcache.New(*addr)
		if err != nil {
			log.Fatalf("Memcache client {%s} not created: %s", *addr, err)
			return
		}
		client.SetTimeout(time.Duration(timeOut) * time.Millisecond)
		messages[devType] = make(chan *ProtobufMes)
		log.Printf("Memcache client ready to connect %s.", *addr)
		go memcWrite(*addr, client, messages[devType], &quit, attempts, dry)
	}

	wg.Add(len(files))

	sort.Strings(files)
	for _, file := range files {
		go readWorker(file, messages, &wg, errors)
	}

	go stats(errors, threshold)

	wg.Wait()
	close(quit)
}

func readWorker(source string, messages map[string]chan *ProtobufMes,
	wg *sync.WaitGroup, errors chan counters) {
	reader, err := os.Open(source)
	if err != nil {
		log.Printf("Error opening the file %s: %s", source, err)
		wg.Done()
		return
	}
	archive, err := gzip.NewReader(reader)
	if err != nil {
		log.Printf("Invalid gzip file %s: %s", source, err)
		wg.Done()
		return
	}
	allCount := 0
	errCount := 0
	scanner := bufio.NewScanner(archive)
	for scanner.Scan() {
		line := scanner.Text()
		apps, err := ParseAppInstalled(line)
		mes, err := apps.Serialize()
		allCount++
		if err != nil {
			errCount++
			continue
		}
		messages[apps.DeviceType] <- mes
	}

	errors <- counters{all: allCount, err: errCount}

	wg.Done()
	archive.Close()
	reader.Close()
	dotRename(source)
}

func memcWrite(addr string, client *memcache.Client,
	messages chan *ProtobufMes, quit *chan bool, attempts int, dry bool) {
	var err error
	var mes *ProtobufMes
	counter := attempts > 0
	for {
		select {
		case mes = <-messages:
			if dry {
				log.Printf("%s - %q", addr, *mes)
			} else {
				for {
					err = client.Set(&memcache.Item{Key: mes.Key, Value: *mes.Value})
					if err != nil {
						if counter {
							attempts--
						} else {
							log.Print(err)
							continue
						}
					} else {
						break
					}
					if attempts == 0 {
						log.Printf("Could not write to memcache %s: %s", addr, err)
						break
					}
				}
			}

		case <-*quit:
			return
		}
	}

}

// ParseAppInstalled парсит строку и возвращает структуру типа AppsInstalled
func ParseAppInstalled(line string) (*AppsInstalled, error) {
	lineparts := strings.Fields(line)
	if len(lineparts) < 5 {
		return nil, fmt.Errorf("Invalid line `%s`", line)
	}
	devicetype := lineparts[0]
	deviceid := lineparts[1]

	lat, err := strconv.ParseFloat(lineparts[2], 64)
	if err != nil {
		lat = 0
		log.Printf("Invalid latitude: `%s`", lineparts[2])
	}

	lon, err := strconv.ParseFloat(lineparts[3], 64)
	if err != nil {
		lon = 0
		log.Printf("Invalid longitude: `%s`", lineparts[3])
	}

	iscomma := func(c rune) bool {
		return c == ','
	}
	stringApps := strings.FieldsFunc(lineparts[4], iscomma)
	apps := make([]uint32, len(stringApps))
	for _, app := range stringApps {
		if appNumber, err := strconv.ParseUint(app, 10, 32); err == nil {
			apps = append(apps, uint32(appNumber))
		} else {
			log.Printf("Invalid app number: `%s`", app)
		}
	}

	return &AppsInstalled{
		DeviceType: devicetype,
		DeviceID:   deviceid,
		Lat:        lat,
		Lon:        lon,
		Apps:       apps,
	}, nil
}

// Serialize сериализует protobuf-сообщение из структуры AppsInstalled
func (apps *AppsInstalled) Serialize() (*ProtobufMes, error) {
	ua := &appsinstalled.UserApps{
		Lat:  &apps.Lat,
		Lon:  &apps.Lon,
		Apps: apps.Apps,
	}

	packed, err := proto.Marshal(ua)
	if err != nil {
		log.Printf("Marshaling error: %s", err)
		return nil, err
	}

	key := fmt.Sprintf("%s:%s", apps.DeviceType, apps.DeviceID)

	return &ProtobufMes{Key: key, Value: &packed}, err
}

func prototest() bool {
	result := true
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
	isline := func(c rune) bool {
		return c == '\n'
	}
	iscomma := func(c rune) bool {
		return c == ','
	}
	for _, line := range strings.FieldsFunc(sample, isline) {
		parts := strings.Fields(line)
		lat, _ := strconv.ParseFloat(parts[2], 64)
		lon, _ := strconv.ParseFloat(parts[3], 64)
		stringApps := strings.FieldsFunc(parts[4], iscomma)
		apps := make([]uint32, len(stringApps))
		for _, app := range stringApps {
			appNumber, _ := strconv.ParseUint(app, 10, 32)
			apps = append(apps, uint32(appNumber))
		}
		ua := &appsinstalled.UserApps{
			Lat:  &lat,
			Lon:  &lon,
			Apps: apps,
		}
		data, err := proto.Marshal(ua)
		if err != nil {
			log.Printf("Marshaling error: %s", err)
			result = false
		}
		unpackedua := &appsinstalled.UserApps{}
		err = proto.Unmarshal(data, unpackedua)
		if err != nil {
			log.Printf("Unmarshaling error: %s", err)
			result = false
		}
		testApps := unpackedua.GetApps()
		for i, app := range ua.GetApps() {
			if app != testApps[i] {
				result = false
				log.Printf("Apps mismatch %d != %d", app, testApps[i])
			}
		}
		if ua.GetLat() != unpackedua.GetLat() {
			log.Printf("Latitude mismatch %f != %f", ua.GetLat(), unpackedua.GetLat())
			result = false
		}
		if ua.GetLon() != unpackedua.GetLon() {
			log.Printf("Longitude mismatch %f != %f", ua.GetLon(), unpackedua.GetLon())
			result = false
		}
	}
	return result
}

func main() {
	memcDevice := make(map[string]*string, 4)
	memcDevice["idfa"] = flag.String("idfa", "127.0.0.1:33013", "memcIdfa")
	memcDevice["gaid"] = flag.String("gaid", "127.0.0.1:33014", "memcGaid")
	memcDevice["adid"] = flag.String("adid", "127.0.0.1:33015", "memcAdid")
	memcDevice["dvid"] = flag.String("dvid", "127.0.0.1:33016", "memcDvid")
	pattern := flag.String("pattern", "D:\\otus_python\\hw9\\data\\appinstalled\\*.tsv.gz", "File name pattern")
	numProc := flag.Int("procs", 4, "Number of go processors")
	attempts := flag.Int("attempts", 0, "Number of attempts before surrendering (0 - endless)")
	dryRun := flag.Bool("dry", false, "Dry run mode")
	test := flag.Bool("test", false, "Test run mode")
	timeOut := flag.Int("timeout", 300, "Socket read/write timeout (msec)")
	threshold := flag.Float64("threshold", 0.001, "Threshold error value")

	log.Print("Memcload started.")

	flag.Parse()

	if *test {
		if !prototest() {
			log.Print("Tests failed.")
		} else {
			log.Print("Tests passed.")
		}
		return
	}

	runtime.GOMAXPROCS(*numProc)

	patternFiles, err := filepath.Glob(*pattern)

	if err != nil || len(patternFiles) == 0 {
		log.Print("Could not get list of files.")
		os.Exit(1)
	}
	files := make([]string, 0)
	for _, path := range patternFiles {
		_, name := filepath.Split(path)
		if name[:1] != "." {
			files = append(files, path)
		}
	}

	if len(files) == 0 {
		log.Print("Files not found. Exit.")
		os.Exit(2)
	}

	dispatch(files, &memcDevice, *attempts, *dryRun, *timeOut, *threshold)
	log.Print("Done.")
}
