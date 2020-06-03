# ADP - Async Data Processing system
ADP - система для распределённой обработки данных, реализованная на корутинах Kotlin.

# Сборка и запуск
Для сборки под Linux необходимо запустить `./gradlew :jar`. Собранный jar можно найти в `build/libs/adp.jar`.

## Запуск в режиме рабочего узла
В режиме рабочего узла jar-файл запускается из отдельной директории, содержащей файл `workers.properties`. 
В этом файле могут быть указаны адрес мастер-узла и размер блока, помещаемого в память по умолчанию во время шаффла.
Пример таких директорий можно найти в папках `workers/worker1` и `workers/worker2`

Запуск в режиме рабочего узла осуществляется со следующими аргументами:

`java -jar adp.jar worker <port>`. 

Рабочий узел после запуска ожидает задачи и выключается по сигналу прерывания (Ctrl+C)

## Запуск в режиме мастер-узла
В режиме тестирования мастер-узла jar-файл запускается из директории, содержащей файл `workers.conf`. Этот файл содержит адреса доступных
рабочих узлов, отделённые переводом строки. Пример такого файла можно найти в `workers.conf` в корне проекта.
Для тестирования распределённого режима необходимо запустить jar-файл со следующими параметрами:

`java -jar adp.jar master <number of workers> <dataset size> <task>`
* `number of workers` - количество рабочих узлов (если указано W, меньшее, чем узлов в файле `workers.conf`, из файла берутся первые W адресов)
* `dataset size` - размер N генерируемого датасета для тестов.
* `task` - имя задания:
  * `count` - Сценарий свёртки по ключу, использующий shuffle между узлами.
  В сценарии генерируются N случайных чисел от 0 до 99, для каждого из которых считается частота,
  после чего выводится самое часто встречающееся число и количество его вхождений.
  * `http` - сценарий параллельных запросов. В сценарии генерируются N случайных чисел от 0 до 9, которые передаются 
  параметром в get-запрос на эхо-сервис https://postman-echo.com. Из результатов обратно извлекаются числа, которые затем складываются в ожин результат.


# API
Задачи предствляют из себя цепочку трансформаций из некоторого датасета в результат некоторой параллельной операции. Пример:

`val totalLength = LinesRDD(LocalMaster(), "words.txt").map { it.length }.reduce { a, b -> a + b }`

Для создания исходного датасета необходим экземпляр класса Master, определяющий режим работы. Это может быть:
* `LocalMaster()` для локального режима
* `GrpcMaster(port: Int, workers: List<String>)` для распределённого режима. Требует указать порт и адреса рабочих узлов.


 Виды исходных датасетов:
* `LinesRDD(master: Master, filename: String)` - датасет строчек из текстового файла с указанным именем. В распределённом режиме каждый узел считывает свой файл относительно своей директории запуска. Таким образом, требуется иметь по файлу с указанным названием  в директории каждого рабочего узла.
* `RandomRDD(master: Master, count: Int)` - генерирует count случайных чисел на каждом узле.
* `fileRDD<T>(master: Master, filename: String)` - считывает значения типа T (требуется указать явно) из бинарного файла, ранее сохранённого системой.

Виды трансформаций:
* `map(f: suspend (T) -> R)` - асинхронно трансформирует каждое значение из датасета. Не гарантирует сохранение порядка.
* `mapSync(f: suspend (T) -> R)` - синхронно трансформирует каждое значение из датасета. Гарантирует сохранение порядка.
* `mapHTTP(f: suspend HttpClient.(T) -> R)` - трансформация с доступом к объекту HttpClient(как this) внутри функции.
* `filter(f: suspend (T) -> Bool)` - асинхронно фильтрует значения по предикату. Не гарантирует сохранение порядка.
* `reduceByKey(comparator: (K, K) -> Int, f: (V, V) -> V)` - сворачивает значения внутри пар с одинаковыми ключами. Пары должны быть типа `NPair`. Такие пары создаются через конструктор или через инфиксную функцию `toN`. (пример: `1 toN "a"`)

Виды параллельных операций:
* `reduce(default: T, f: (T, T) -> T)` - свёртка датасета в одно значение.
* `saveAsObject(filename: String)` - сохранение датасета в бинарный файл для последующего чтения системой. 
* `saveAsText(filename: String)` - сохранение датасета в текстовый файл.
