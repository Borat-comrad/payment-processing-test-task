# Асинхронный процессинг платежей

FastAPI + Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ через
FastStream, Alembic. Таблицы `payments` и `outbox`, один consumer,
три попытки обработки и очередь `payments.dlq`.

## Запуск

Нужны Docker и Docker Compose. Из корня проекта в PowerShell:

```powershell
Copy-Item .env.example .env
docker compose up -d --build --wait
```

В Bash: `cp .env.example .env`. В `.env` задайте `API_KEY` длиной минимум
16 символов. Ключ из примера предназначен для локального запуска.
Миграции применяются автоматически перед запуском API.

| Сервис | Адрес на хосте |
| --- | --- |
| API | `http://localhost:58000` |
| RabbitMQ Management | `http://localhost:55673`, логин/пароль `payments` / `payments` |
| PostgreSQL | `localhost:55432`, БД/логин/пароль `payments` |
| AMQP | `localhost:55672` |

Порты доступны только с локальной машины. Для их изменения задайте в `.env`
`API_PORT`, `POSTGRES_PORT`, `RABBITMQ_PORT`, `RABBITMQ_UI_PORT`.
Остановка с сохранением данных: `docker compose down`.

## Создание и получение платежа

Все маршруты требуют заголовок `X-API-Key`, создание — также `Idempotency-Key`.
Замените URL webhook на адрес своего HTTP-получателя, доступный из consumer.
`localhost` в этом URL означает контейнер consumer.

```powershell
$headers = @{
    'X-API-Key' = 'local-development-key-change-me'
    'Idempotency-Key' = 'order-42'
}
$body = @{
    amount = '125.50'
    currency = 'RUB'
    description = 'Order 42'
    metadata = @{ order_id = 42 }
    webhook_url = 'https://your-server.example/payments/webhook'
} | ConvertTo-Json

$payment = Invoke-RestMethod -Method Post -Uri http://localhost:58000/api/v1/payments -Headers $headers -ContentType 'application/json' -Body $body
$payment
Invoke-RestMethod -Uri "http://localhost:58000/api/v1/payments/$($payment.payment_id)" -Headers $headers
```

`POST /api/v1/payments` возвращает `202 Accepted`:

```json
{
  "payment_id": "6ea83cfa-9e9c-42cc-9beb-350349e16b09",
  "status": "pending",
  "created_at": "2026-10-23T12:00:00Z"
}
```

`GET /api/v1/payments/{payment_id}` возвращает `200` и дополнительно поля
`amount`, `currency`, `description`, `metadata`, `webhook_url`, `processed_at`.
До завершения обработки `processed_at = null`.

- Сумма: больше нуля, максимум `9999999999999999.99`, без округления.
  В ответах передаётся строкой; в запросах строка также рекомендуется.
- Валюта: `RUB`, `USD`, `EUR`. Описание: 1–1000 символов.
- Метаданные: JSON-объект, по умолчанию `{}`.
- `Idempotency-Key`: 1–200 печатных ASCII-символов без пробелов.
- Webhook URL: HTTP/HTTPS, до 2048 символов. Время — UTC.
- Ошибки: `401` — ключ API; `404` — платёж не найден; `409` — конфликт
  идемпотентности; `422` — неверные данные; `503` — ошибка работы с БД.

## Обработка, outbox и повторы

Платёж и начальное событие сохраняются одной транзакцией. Фоновая задача
в процессе consumer публикует outbox в `payments.new`, используя persistent
messages, publisher confirms и mandatory routing. При недоступности брокера
событие остаётся в БД до успешной публикации.

Один обработчик эмулирует шлюз (2–5 секунд, 90% успеха, 10% отказов), сохраняет
`succeeded` или `failed`, затем отправляет HTTP POST на webhook:

```json
{
  "payment_id": "6ea83cfa-9e9c-42cc-9beb-350349e16b09",
  "amount": "125.50",
  "currency": "RUB",
  "status": "succeeded",
  "processed_at": "2026-10-23T12:00:03+00:00"
}
```

Любой `2xx` подтверждает доставку. Остальные статусы, сетевые ошибки и таймаут
приводят к повтору. Срок HTTP-запроса — максимум пять секунд; перенаправления
отключены. Результат платежа сохраняется до webhook и повторно не вычисляется.

Всего три попытки: первая сразу, следующая через 1 секунду, последняя через
2 секунды после ошибки предыдущей. Дополнительно возможны ожидание очереди и
до 250 мс интервала опроса outbox. Следующая попытка либо событие для DLQ
сохраняются атомарно с закрытием текущей попытки. ACK отправляется после коммита.
После третьей ошибки сообщение публикуется в `payments.dlq`.

## Допущения и границы гарантий

- Повтор ключа с тем же нормализованным запросом возвращает `202`, прежний ID
  и текущий статус. Изменённое тело с тем же ключом — `409`. Ключи без TTL.
- 10% отказов шлюза — окончательный `failed`, о котором тоже отправляется webhook.
  Ошибка доставки уведомления не меняет итоговый статус платежа.
- Доставка событий — как минимум один раз. Повтор обработанного события
  распознаётся по outbox ID; получатель webhook должен устранять дубли по
  `payment_id`. Потеря HTTP-ответа или авария до коммита допускает повтор запроса.
- Недоступность БД не расходует прикладные попытки: consumer удерживает сообщение
  без ACK до восстановления. При трёх ошибках до получения результата шлюза
  платёж может остаться `pending`, а событие попадёт в DLQ.
- Гарантии предполагают сохранность данных PostgreSQL и RabbitMQ. В Compose
  по одному узлу; кластерная отказоустойчивость и автоматический разбор DLQ
  не реализованы. HTTP webhook рассчитан на доверенного владельца API key.

## Проверка работы

```powershell
docker compose ps
docker compose logs --tail 50 consumer
docker compose exec api alembic check
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

После создания платежа GET должен показать `succeeded` или `failed`, а получатель —
уведомление с тем же ID. Повтор запроса с прежним ключом возвращает тот же платёж.
Если получатель постоянно возвращает ошибку, после трёх попыток сообщение
появится в `payments.dlq`; очередь также видна в RabbitMQ Management.
