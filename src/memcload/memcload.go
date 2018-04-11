package main

import (
	"errors"
	"log"
	"appsinstalled"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"github.com/golang/protobuf/proto"
	"strings"
	"strconv"
)

// Структура для хранения задачи в процессе ее выполнения
type Task struct {
	wg        *sync.WaitGroup
	Attempt   int
	Name      string
	Error     error
	isSuccess bool
}

// Структура, в которой храниться распаршенная запись из файла логов
type AppsInstalled struct {
	DeviceType string
	DeviceID   string
	Lat        float64
	Lon        float64
	Apps       []uint32
}

// Алиас к wg.Done. Нужно жать когда работа с задачей завершена
func (task *Task) Done() {
	task.doneOnce.Do(func() {
		task.wg.Done()
	})
}

// Проверка что задача завершена. Либо успешно выполнена, либо закончились попытки
func (task *Task) IsFinished() bool {
	if task.isSuccess {
		return true
	}

	if task.Attempt > 5 { // @TODO: Config?
		return true
	}

	return false
}

// Вызывается при неуспешном завершении.
// Инкрементит количество попыток и запоминает ошибку
func (task *Task) Fail(err error) {
	task.Attempt++
	task.Error = err
}

// Помечает задачу как выполненную. При этом нужно создать файл с mtime
func (task *Task) Success() {
	task.Error = nil
	task.isSuccess = true
}

// Горутина-воркер. Берет из queue задачу и пытается обработать.
// В результате либо завершает ее, либо кладет в конец очереди на повторную обработку
func worker(queue chan *Task, exit chan bool) {
	for {
		select {
		case task := <-queue:
			task.logger.Info("start")
			if err := handleFile(task.DataRoot, task.URL); err != nil {
				task.Fail(err)
				task.logger.Info("fail", zap.Int("attempt", task.Attempt), zap.Error(err))
			} else {
				task.Success()
				task.logger.Info("success")
			}

			if task.IsFinished() {
				task.Done()
			} else {
				queue <- task
			}
		case <-exit:
			return
		}
	}
}

// создание задач
func GetTasks(files []string) ([]*Task, error) {
	var wg sync.WaitGroup
	tasks := make([]*Task, 0)

	if err != nil {
		return tasks, err
	}

	for _, file := range files {
		newJob := &Task{
			wg:   &wg,
			Name: file.Name,
		}

		tasks = append(tasks, newJob)
		wg.Add(1)
	}

	return tasks, nil
}

func (apps *AppsInstalled) Serialize() (string, []*byte, error) {
	ua := &appsinstalled.UserApps{
		Lat: apps.Lat,
		Lon: apps.Lon,
		Apps: apps.Apps
		}

	packed, err := proto.Marshal(ua)
	if err != nil {
		log.Fatal("Marshaling error: ", err)
	}

	key := fmt.Sprintf("%s:%s", apps.DeviceType, apps.DeviceID)

	return key, packed, err
}

func ParseAppInstalled(line string) (*AppsInstalled, error) {
	lineparts := strings.Fields(line)
	err := nil
	if len(parts) < 5 {
		return nil, errors.New("Invalid line `%s`", line)
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
	for _, app range stringApps {
		if appNumber, err := strconv.ParseUint(app, 10, 32); err == nil {
			apps = append(apps, uint32(appNumber))
		} else {
			log.Fatalf("Invalid app number: `%s`", app)
		}
	}

	return &AppsInstalled{
		DeviceType: devicetype,
		DeviceID: deviceid,
		Lat: lat,
		Lon: lon,
		Apps: apps
		}, nil
}

func main() {
	memcDevice = make(map[string]*string)
	memcDevice["idfa"] = flag.String("idfa", "127.0.0.1:33013", "memcIdfa")
	memcDevice["gaid"] = flag.String("gaid", "127.0.0.1:33014", "memcGaid")
	memcDevice["adid"] = flag.String("adid", "127.0.0.1:33015", "memcAdid")
	memcDevice["dvid"] = flag.String("dvid", "127.0.0.1:33016", "memcDvid")
	pattern = flag.String("pattern", "./data/appsinstalled/*.tsv.gz", "pattern")
	numProc = flag.String("numProc", "4", "nCPU")
	flag.Parse()

	runtime.GOMAXPROCS(*numProc)

	files, err = filepath.Glob(*pattern)

	if err != nil {
		log.Fatal("No matching for pattern. Exit.")
		os.Exit(1)
	}

	// Составляет список задач
	tasks, err := getTasks(files)

	if err != nil {
		log.Fatal(err.Error())
		return
	}

	if tasks == nil || len(tasks) == 0 {
		log.Fatal("No files to load.")
		return
	}

	wg := tasks[0].wg

	// делаем очередь задач из массива
	queue := make(chan *Task, len(tasks))

	for _, task := range tasks {
		queue <- task
	}

	workerExit := make(chan bool)
	defer close(workerExit)

	for i := 0; i < cfg.Workers; i++ {
		go func() {
			worker(queue, workerExit)
		}()
	}

	// ожидать завершение всех воркеров
	wg.Wait()
}
