1. Настрой подключение к бакету nano ~/.config/rclone/rclone.conf. Либо файл отредактирую, либо через rclone config
Пример конфигурации для Yandex S3:
type: s3
provider: Other
env_auth: false
access_key_id: YOUR_ACCESS_KEY
secret_access_key: YOUR_SECRET_KEY
region: ru-central1
endpoint: storage.yandexcloud.net
acl: private

[ya]
type = s3
provider = Other
access_key_id = YOUR_ACCESS_KEY
secret_access_key = YOUR_SECRET_KEY
region = ru-central1
endpoint = storage.yandexcloud.net
acl = private

Проверить что есть соединение: rclone ls ya:otus-bucket-20251311-b1ghiv85eubrk846dis6


2. Скачать из бакета в локаль: 
rclone copy ya:otus-bucket-20251311-b1ghiv85eubrk846dis6 s3/ --progress --transfers=16 --checkers=16 --fast-list

3. Загрузуить из локали в бакет: 
rclone copy s3/ ya:otus-bucket-20251311-b1ghiv85eubrk846dis6 --progress --transfers=16 --checkers=16