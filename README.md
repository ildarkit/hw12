# MemcLoad v2

### Версия MemcLoad на go.
Программа парсит и заливает в мемкеш поминутную выгрузку логов трекера установленных приложений. Ключом является тип и идентификатор
устройства через двоеточие, значением являет protobuf сообщение (https://github.com/golang/protobuf).
#### Установка зависимостей
  - $ go get github.com/golang/protobuf/proto
  - $ go get github.com/rainycape/memcache

#### Аргументы командной строки:

Аргумент|Описание
---|---
--pattern                       |шаблон загружаемого файла
--test                          |тестовый режим
--dry                           |режим с выводом в лог без записи в мемкеш
--idfa, --gaid, --adid, --dvid  |адреса мемкеш для соответствующих типов устройств
--threshold                     |пороговое значение ошибок парсинга
--timeout                       |таймаут на запись/чтение в сокет (мс)
--attempts                      |количество попыток записи в мемкеш (0 - бесконечно)
--help                          |справка
