package main

import (
	//"appsinstalled"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// Структура для хранения задачи в процессе ее выполнения
type Task struct {
	sync.RWMutex
	wg       *sync.WaitGroup
	doneOnce sync.Once
	Attempt  int
	Name     string
}

// Алиас к wg.Done. Нужно жать когда работа с задачей завершена
func (task *Task) Done() {
	task.doneOnce.Do(func() {
		task.wg.Done()
	})
}

// Проверка что задача завершена. Либо успешно выполнена, либо закончились попытки
func (task *Task) IsFinished() bool {
	task.RLock()
	defer task.RUnlock()

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
	task.Lock()
	defer task.Unlock()

	task.Attempt += 1
	task.Error = err
}

// Проверяет нужно ли перекачивать файл
// Сравнивает mtime файла из хадупа с mtime локального файла
// updated/part-00000.gz-mtime
func (task *Task) IsUpdated() bool {
	p, _ := url.Parse(task.URL)
	_, file := path.Split(p.Path)

	mtimeFile := path.Join(task.DataRoot, "updated", fmt.Sprintf("%s-mtime", file))

	stat, err := os.Stat(mtimeFile)

	if err != nil {
		return false
	}

	if stat.ModTime().Unix() == task.SourceTime/1000 {
		return true
	}

	return false
}

// Помечает задачу как выполненную. При этом нужно создать файл с mtime
func (task *Task) Success() {
	task.Lock()
	defer task.Unlock()

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
func getTasks(files []string) ([]*Task, error) {
	var wg sync.WaitGroup
	tasks := make([]*Task, 0)

	if err != nil {
		return tasks, err
	}

	for _, file := range files {
		if !strings.HasPrefix(file.Name, "part-") {
			continue
		}

		fileURL := file.OpenURL()
		if useMirrors {
			fileURL = file.MirrorURL()
		}
		newJob := &Task{
			wg:   &wg,
			Name: file.Name,
		}

		// Пропустить файлы, которые не нужно качать так как они и так последней версии
		if newJob.IsUpdated() {
			continue
		}

		tasks = append(tasks, newJob)
		wg.Add(1)
	}

	return tasks, nil
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
		fmt.Print("No matching for pattern. Exit.")
		os.Exit(1)
	}

	// Составляет список задач
	tasks, err := getTasks(files)

	if err != nil {
		fmt.Print(err.Error())
		return
	}

	if tasks == nil || len(tasks) == 0 {
		fmt.Print("No files to load.")
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
