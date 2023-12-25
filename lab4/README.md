# Лабораторная 4

- В Windows запустите сервер двойным кликом по скрипту zkServer.cmd в папке ./bin/ или из терминала, набрав zkServer.cmd

![image](https://github.com/stillysyw/Big_Data/assets/154344530/5667c044-5535-4f50-ad14-5a3dcb0d5f99)


## Взаимодействие с ZooKeeper через командный интерфейс CLI

- Устанавливаем подключение к ZooKeeper CLI сессии:

![image](https://github.com/stillysyw/Big_Data/assets/154344530/c985a0e9-b758-4284-ac21-a77f1913e567)

- Для вывода всех возможных команд наберите help:

![image](https://github.com/stillysyw/Big_Data/assets/154344530/6dfe03fd-1d30-4404-9e32-0d0a118d6d48)

- Находясь в консоли CLI введите команду ls /

![image](https://github.com/stillysyw/Big_Data/assets/154344530/0a358c95-ceed-482b-821b-6233ee14e959)

- создайте свой узел /mynode с данными "first_version" следующей командой: create /mynode 'first_version'

![image](https://github.com/stillysyw/Big_Data/assets/154344530/8896f3e7-886e-415a-8558-22aef8f3ffa8)

- Проверьте, что в корне появился новый узел.

![image](https://github.com/stillysyw/Big_Data/assets/154344530/36ad3f3a-d9d0-407b-abf4-2348a365b21c)

- Следующие команды возвращают данные и метаданные узла: get /mynode stat /mynode

![image](https://github.com/stillysyw/Big_Data/assets/154344530/8042f1d5-13b8-434e-b28f-812e876442cf)

- Измените данные узла на "second_version":

![image](https://github.com/stillysyw/Big_Data/assets/154344530/e1d8778d-12f1-4716-a7af-41c25b0ecd29)

- Теперь создайте два нумерованных (sequential) узла в качестве дочерних mynode

![image](https://github.com/stillysyw/Big_Data/assets/154344530/4b1c86fa-16ce-4e22-bed0-95e952b2429c)

- Внутри CLI сессии, создайте узел mygroup

![image](https://github.com/stillysyw/Big_Data/assets/154344530/bfe55930-192b-40eb-865f-893656d401e5)

- Откройте две новых CLI консоли и в каждой создайте по дочернему узлу в mygroup 

![image](https://github.com/stillysyw/Big_Data/assets/154344530/4c2c2bf9-e054-485c-b67d-6786cc901e5c)

- Проверьте в исходной консоли, что grue и bleen являются членами группы mygroup

![image](https://github.com/stillysyw/Big_Data/assets/154344530/15d60c57-d7ba-4989-b558-9d92ecdefc6a)

-  Выберите консоль grue и обратитесь к информации узла bleen.

![image](https://github.com/stillysyw/Big_Data/assets/154344530/b055664a-2735-4ea9-90b8-4707330f4667)

- Нажмите сочетание клавиш Ctrl+D в одной из консолей, создавшей эфимерный узел. Проверьте, что соответствующий узел пропал из mygroup.

![image](https://github.com/stillysyw/Big_Data/assets/154344530/99bdbd8d-f824-477e-8ec0-5443538967b7)

- В заключении удалите узел /mygroup.

![image](https://github.com/stillysyw/Big_Data/assets/154344530/53ff95b9-ca08-4026-8104-aa2ac66fdadc)


## Пример управления конфигурацией распределённого приложения
- Создадим в корне узел "myconfig" в задачу которого будет входить хранение конфигурации. В нашем случае узел будет хранить строку 'sheep_count=1'.Откройте новую консоль и подключитесь к ZooKeeper. Данная консоль будет играть роль физического сервера, который ожидает получить оповещение в случае изменения конфигурационной информации, записанной в /myconfig znode. Следующая команда устанавливает watch-триггер на узел: get /myconfig -w true

![image](https://github.com/stillysyw/Big_Data/assets/154344530/fac03f95-df41-4b90-8362-23740d81961d)

- Вернитесь к первому терминалу и измените значение myconfig. Во втором терминале должно появиться оповещение об изменении данных!

![image](https://github.com/stillysyw/Big_Data/assets/154344530/5bda9e81-0e0e-4b6a-8a72-839dc5867aa1)

- Удалите узел /myconfig

![image](https://github.com/stillysyw/Big_Data/assets/154344530/93e73506-14b1-43bf-a325-7b1e6ddd8933)



### Philosophers

Результат:
```
Philosopher 0 is going to eat
Philosopher 4 is going to eat
Philosopher 1 is going to eat
Philosopher 2 is going to eat
Philosopher 3 is going to eat
Philosopher 2 takes left fork
Philosopher 2 takes right fork
Philosopher 5 takes left fork
Philosopher 5 takes right fork
Philosopher 4 takes left fork
Philosopher 5 puts right fork
Philosopher 5 puts left fork 
Philosopher 4 takes right fork
Philosopher 5 is thinking
Philosopher 1 takes left fork
Philosopher 2 puts right fork
Philosopher 2 puts left fork 
Philosopher 1 takes right fork
Philosopher 3 takes left fork
Philosopher 2 is thinking
Philosopher 4 puts right fork
Philosopher 1 puts right fork
Philosopher 1 puts left fork 
Philosopher 4 puts left fork 
Philosopher 3 takes right fork
Philosopher 1 is thinking
Philosopher 4 is thinking
Philosopher 4 is going to eat
Philosopher 5 takes left fork
Philosopher 5 takes right fork
Philosopher 1 is going to eat
Philosopher 2 takes left fork
Philosopher 5 puts right fork
Philosopher 5 puts left fork 
Philosopher 1 takes left fork
Philosopher 5 is thinking
Philosopher 3 is going to eat
Philosopher 3 puts right fork
Philosopher 3 puts left fork
Philosopher 2 takes right fork
Philosopher 3 is thinking
Philosopher 4 takes left fork
Philosopher 4 takes right fork
Philosopher 2 puts right fork
Philosopher 2 puts left fork
Philosopher 1 takes right fork
Philosopher 2 is thinking
Philosopher 2 is going to eat
Philosopher 3 takes left fork
Philosopher 1 puts right fork
Philosopher 1 puts left fork
Philosopher 1 is thinking
Philosopher 4 puts right fork
Philosopher 4 puts left fork
Philosopher 3 takes right fork
Philosopher 4 is thinking
Philosopher 3 puts right fork
Philosopher 3 puts left fork
Philosopher 3 is thinking
```
## Двуфазный коммит протокол для high-available регистра
Координатор уведомляет исполнителей (client) о начале транзакции. После этого координатор устанавливает слежение (WATCH) на путь /app/tx для отслеживания изменений транзакционного узла. Каждый исполнитель создает временный узел /app/tx/node_i, в котором содержится решение о том, следует ли выполнить (commit) или отменить (abort) транзакцию. Затем исполнитель подписывается на события, связанные с его узлом, и ожидает решения от координатора на втором этапе. На этом этапе координатор принимает окончательное решение (основанное на большинстве голосов) о том, следует ли подтвердить (commit) или отклонить (abort) транзакцию. Это решение принимается после ожидания таймаута или создания всех временных узлов исполнителей с решением commit. Затем координатор обновляет значения временных узлов каждого исполнителя на commit или abort. Исполнители, в зависимости от принятого решения, либо выполняют, либо отменяют транзакцию, а затем обновляют значение узла до статуса committed.
