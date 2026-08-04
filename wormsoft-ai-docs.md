# Wormsoft AI — Документация LLM Provider

<!-- audit-status:2026-08-04 -->
> **🟡 ЧАСТИЧНО УСТАРЕЛО · сверено 2026-08-04.**
> Основа верна, но часть утверждений разошлась с рабочим стеком. Сверяйтесь с разбором, прежде чем опираться на числа и команды.
> Конкретных расхождений найдено: **5** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](docs/AUDIT-2026-08-04.md).

> Источник: https://ai.wormsoft.ru/docs/  
> Агентские модели по себестоимости

---

## Содержание

1. [Обзор](#обзор)
2. [Аутентификация](#аутентификация)
3. [Список моделей](#список-моделей)
4. [Responses API](#responses-api)
5. [Chat Completions API](#chat-completions-api)
6. [Embeddings API](#embeddings-api)
7. [Models API](#models-api)
8. [Ошибки и лимиты](#ошибки-и-лимиты)
9. [Ценовая политика](#ценовая-политика)
10. [Подписки](#подписки)

---

## Обзор

OpenAI-compatible слой для работы с generation-моделями, multimodal input, embeddings и каталогом моделей.

**Основной публичный base URL:** `https://ai.wormsoft.ru/api/gpt`

| Параметр | Значение |
|---|---|
| Primary URL | `https://ai.wormsoft.ru/api/gpt` |
| Доп. алиасы | `/gpt/v1` · `/gpt/v1/v1` |
| Аутентификация | `Authorization: Bearer API_KEY` |

### Возможности

- **OpenAI-compatible API** — поддерживаются familiar endpoints для responses, chat completions, models и embeddings
- **Text + image input** — публичные generation-модели принимают текстовые и мультимодальные входы в OpenAI-style формате
- **Fallback for stability** — при редких сбоях сервис может прозрачно переключить обработку на резервный маршрут, сохраняя прозрачность ответа

### Публичные endpoint-ы

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/responses` | Главный универсальный endpoint для генерации ответов и мультимодальных сценариев |
| `POST` | `/chat/completions` | Совместимый слой для messages, tools и tool calling |
| `POST` | `/embedding` | Получение embedding-векторов для текста |
| `GET` | `/models` | Discovery endpoint для получения каталога доступных моделей |

### Quick Start

```bash
curl --request POST \
  --url https://ai.wormsoft.ru/api/gpt/responses \
  --header 'Authorization: Bearer YOUR_API_KEY' \
  --header 'Content-Type: application/json' \
  --data '{
    "model": "openai/gpt-5.4-mini",
    "input": "Привет"
  }'
```

### Ошибки и лимиты (краткая сводка)

- `429` возвращается, если пользователь достиг лимита
- `400` возвращается при несовместимых параметрах
- Для `/responses` параметр `store: true` не поддерживается
- Фактическая модель ответа остаётся прозрачной для клиента

---

## Аутентификация

> https://ai.wormsoft.ru/docs/llm/authentication

Все LLM endpoint-ы сервиса используют единый публичный base URL и единый способ авторизации через Bearer API key.

| Параметр | Значение |
|---|---|
| Base URL | `https://ai.wormsoft.ru/api/gpt` |
| Primary integration path | Используйте только основной путь без `v1` в новой интеграции |
| Legacy aliases | Пути `/gpt/v1` и `/gpt/v1/v1` сохранены только для обратной совместимости |

### Authorization header

Каждый запрос должен содержать Bearer token. Без валидного ключа endpoint не будет обработан.

```
Authorization: Bearer YOUR_API_KEY
```

### Минимальный пример запроса

```bash
curl --request POST \
  --url https://ai.wormsoft.ru/api/gpt/responses \
  --header 'Authorization: Bearer YOUR_API_KEY' \
  --header 'Content-Type: application/json' \
  --data '{
    "model": "openai/gpt-5.4-mini",
    "input": "Привет"
  }'
```

### Что важно помнить

- Один и тот же API key используется для всех LLM endpoint-ов
- Форматы запросов документируются как OpenAI-compatible
- В документации не описывается процесс выдачи ключа
- Для production-интеграции используйте только публичный base URL

---

## Список моделей

> https://ai.wormsoft.ru/docs/llm/models-overview

Сервис поддерживает generation-модели для текста и multimodal input, а также отдельную embedding-модель.

### Доступные generation-модели

- `google/gemma4:26b`
- `google/gemma4:31b`
- `openai/gpt-5.2`
- `openai/gpt-5.2-codex`
- `openai/gpt-5.3-codex`
- `openai/gpt-5.4`
- `openai/gpt-5.4-mini`
- `openai/gpt-5.5`
- `qwen/qwen3.5-35b`
- `qwen/qwen3.5-plus`
- `qwen/qwen3.6-plus`
- `wormsoft/agent/high`
- `wormsoft/agent/low`
- `wormsoft/agent/medium`
- `wormsoft/code/high`
- `wormsoft/code/low`
- `wormsoft/code/medium`
- `zai/glm-5.1`

### Capabilities

- Generation-модели принимают `text` и `image` input
- Output для generation-моделей — `text`
- Vision-capability отражена в публичном каталоге моделей
- Embedding-модель документируется отдельно от generation flow

### Прозрачность ответа

Для клиента важно ориентироваться на фактическую модель, которая реально обработала запрос. Это особенно важно в редких сценариях fallback-маршрутизации.

---

## Responses API

> https://ai.wormsoft.ru/docs/llm/responses

Основной универсальный endpoint для генерации ответов, text input и multimodal сценариев.

```
POST https://ai.wormsoft.ru/api/gpt/responses
```

Legacy aliases: `/api/gpt/v1/responses` и `/api/gpt/v1/v1/responses`

### Формат запроса

- Документируйте request body как OpenAI-compatible Responses API
- Обязательно передаётся `model`
- Основной вход — поле `input`
- Поддерживаются text input, multimodal input и stream

### Минимальный пример

```bash
curl --request POST \
  --url https://ai.wormsoft.ru/api/gpt/responses \
  --header 'Authorization: Bearer YOUR_API_KEY' \
  --header 'Content-Type: application/json' \
  --data '{
    "model": "openai/gpt-5.4-mini",
    "input": [
      {
        "role": "user",
        "content": [
          {
            "type": "input_text",
            "text": "Привет"
          }
        ]
      }
    ],
    "stream": false
  }'
```

### Multimodal input

Для vision-сценариев используйте OpenAI-style content blocks. Документация не вводит отдельный кастомный формат.

```json
{
  "model": "openai/gpt-5.4",
  "input": [
    {
      "role": "user",
      "content": [
        {
          "type": "input_text",
          "text": "Опиши изображение"
        },
        {
          "type": "input_image",
          "image_url": "https://example.com/image.png"
        }
      ]
    }
  ]
}
```

### Streaming

Для streaming передайте `stream: true`. Формат потока следует трактовать как OpenAI-compatible streaming response.

```json
{
  "model": "openai/gpt-5.4-mini",
  "input": "Привет",
  "stream": true
}
```

### Ограничения и ошибки

- `store: true` не поддерживается и приводит к `400 Bad Request`
- `429` возвращается при достижении пользовательского лимита
- `500` возможен, если запрос не обработан даже после резервной попытки
- При несовместимых параметрах запроса возможен `400`

### Fallback behavior

При редких технических ошибках сервис может использовать резервный маршрут выполнения. Для клиента это должно оставаться прозрачным: в ответе важно ориентироваться на фактическую модель, которая реально обработала запрос.

---

## Chat Completions API

> https://ai.wormsoft.ru/docs/llm/chat-completions

Совместимый endpoint для сообщений, tools и tool calling в familiar OpenAI-style формате.

```
POST https://ai.wormsoft.ru/api/gpt/chat/completions
```

Legacy aliases: `/api/gpt/v1/chat/completions` и `/api/gpt/v1/v1/chat/completions`

### Request shape

- Request body документируется как OpenAI-compatible Chat Completions API
- Обязательные high-level поля: `model` и `messages`
- Поддерживаются `tools`, `tool calls` и `stream`

### Минимальный пример

```bash
curl --request POST \
  --url https://ai.wormsoft.ru/api/gpt/chat/completions \
  --header 'Authorization: Bearer YOUR_API_KEY' \
  --header 'Content-Type: application/json' \
  --data '{
    "model": "openai/gpt-5.4-mini",
    "messages": [
      {
        "role": "user",
        "content": "Привет"
      }
    ],
    "stream": false
  }'
```

### Tool calling

Для agent-like сценариев передавайте tools в OpenAI-style формате.

```json
{
  "model": "openai/gpt-5.4-mini",
  "messages": [
    {
      "role": "user",
      "content": "Какая погода в Москве?"
    }
  ],
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "get_weather",
        "description": "Get current weather",
        "parameters": {
          "type": "object"
        }
      }
    }
  ]
}
```

**Response with tool calls:**

```json
{
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": null,
        "tool_calls": [
          {
            "id": "call_1",
            "type": "function",
            "function": {
              "name": "get_weather",
              "arguments": "{}"
            }
          }
        ]
      },
      "finish_reason": "tool_calls"
    }
  ]
}
```

### Streaming

Для incremental вывода передайте `stream: true` и обрабатывайте OpenAI-compatible chunk flow.

### Ошибки и ограничения

- `400` возвращается для unsupported model или некорректного request body
- `429` возвращается при достижении пользовательского лимита
- Для интегратора важно ориентироваться на фактическую модель ответа

---

## Embeddings API

> https://ai.wormsoft.ru/docs/llm/embeddings

Endpoint для получения embedding-векторов. Поддерживает два алиаса: `/embedding` и `/embeddings`.

```
POST https://ai.wormsoft.ru/api/gpt/embedding
POST https://ai.wormsoft.ru/api/gpt/embeddings
```

**Поддерживаемая модель:** `qwen/qwen3-embedding:8b`

### Что делает endpoint

Возвращает векторное представление входного текста. Этот endpoint не предназначен для генерации ответа ассистента.

### Минимальный пример

```bash
curl --request POST \
  --url https://ai.wormsoft.ru/api/gpt/embedding \
  --header 'Authorization: Bearer YOUR_API_KEY' \
  --header 'Content-Type: application/json' \
  --data '{
    "model": "qwen/qwen3-embedding:8b",
    "content": "Что такое машинное обучение?"
  }'
```

### Response example

```json
{
  "object": "list",
  "data": [
    {
      "object": "embedding",
      "embedding": [0.0123, -0.112, 0.334, "..."],
      "index": 0
    }
  ],
  "model": "qwen/qwen3-embedding:8b"
}
```

### Ошибки и ограничения

- `429` возвращается при достижении лимита пользователя
- `400` возможен для unsupported model или некорректного request body
- Для первой интеграции достаточно использовать endpoint `/embedding`

---

## Models API

> https://ai.wormsoft.ru/docs/llm/models

Discovery endpoint для получения каталога доступных моделей и их основных capabilities.

```
GET https://ai.wormsoft.ru/api/gpt/models
```

### Что возвращает endpoint

- Структуру `list` с массивом моделей
- Для каждой модели — fields вроде `id`, `modalities`, `input_modalities`, `output_modalities`, `capabilities`
- Удобный источник для UI выбора модели и фильтрации capabilities

### Пример запроса

```bash
curl --request GET \
  --url https://ai.wormsoft.ru/api/gpt/models \
  --header 'Authorization: Bearer YOUR_API_KEY'
```

### Пример ответа

```json
{
  "object": "list",
  "data": [
    {
      "id": "openai/gpt-5.4",
      "object": "model",
      "input_modalities": ["text", "image"],
      "output_modalities": ["text"],
      "capabilities": {
        "vision": true
      }
    }
  ]
}
```

### Когда использовать

- При построении model selector на фронтенде
- Для отображения capabilities перед отправкой запроса
- Для справочного каталога моделей внутри документации

---

## Ошибки и лимиты

> https://ai.wormsoft.ru/docs/llm/errors-and-limits

Сводная страница по публичному поведению сервиса при превышении лимитов, несовместимых параметрах и редких внутренних сбоях.

### 429 Limit reached

Если пользователь достиг лимита, сервис не выполняет запрос и возвращает `429` с сообщением `user reached the limit`.

### 400 Bad Request

Возвращается при unsupported model, несовместимых параметрах или специальных ограничениях вроде `store: true` для `/responses`.

### Матрица ошибок

| Endpoint | Status | Meaning |
|---|---|---|
| `/responses` | `400` | Unsupported params, incompatible request body, `store: true` |
| `/responses` | `429` | User reached the limit |
| `/responses` | `500` | Primary and fallback processing both failed |
| `/chat/completions` | `400` | Unsupported model or invalid request body |
| `/chat/completions` | `429` | User reached the limit |
| `/embedding` | `400` | Unsupported model or invalid request body |
| `/embedding` | `429` | User reached the limit |
| `/models` | `401`/`403` | Authorization issues |

### Fallback and transparency

Сервис использует резервные механизмы для повышения стабильности. Fallback — это редкий защитный сценарий, а не обычная маршрутизация. Для клиента важна прозрачность: ориентируйтесь на фактическую модель, указанную в ответе.

---

## Ценовая политика

> https://ai.wormsoft.ru/docs/llm/pricing-and-usage

Стоимость указана в **кредитах** за 1 000 000 токенов. Кредиты списываются при использовании моделей; лимиты кредитов по подписке выдаются отдельно.

### Таблица тарифов

| Model | Input tokens | Output tokens | Cache tokens |
|---|---|---|---|
| `anthropic/claude-opus-4.6` | 30 000 000 | 150 000 000 | 2 000 000 |
| `anthropic/claude-opus-4.7` | 1 000 000 | 2 000 000 | 3 000 000 |
| `anthropic/claude-sonnet-4.6` | 30 000 000 | 150 000 000 | 2 000 000 |
| `arcee/trinity-large-preview-free` | 1 000 000 | 2 000 000 | 3 000 000 |
| `deepseek-ai/deepseek-v3.1` | 80 000 | 3 000 000 | 20 000 |
| `deepseek-ai/deepseek-v3.2` | 1 000 000 | 2 000 000 | 3 000 000 |
| `deepseek-ai/deepseek-v4-flash` | 1 000 000 | 2 000 000 | 3 000 000 |
| `deepseek-ai/deepseek-v4-pro` | 1 000 000 | 2 000 000 | 3 000 000 |
| `google/gemini-3-flash-preview` | 1 000 000 | 2 000 000 | 3 000 000 |
| `google/gemini-3.1-pro` | 2 000 000 | 5 000 000 | 200 000 |
| `google/gemma3:12b` | 1 000 000 | 2 000 000 | 3 000 000 |
| `google/gemma3:27b` | 1 000 000 | 2 000 000 | 3 000 000 |
| `google/gemma4:26b` | 500 | 5 000 | 50 |
| `google/gemma4:31b` | 4 000 | 50 000 | 500 |
| `gpt-5.2` | 40 000 | 2 500 000 | 20 000 |
| `gpt-5.2-codex` | 40 000 | 2 500 000 | 20 000 |
| `kimi/kimi-k2-thinking` | 1 000 000 | 2 000 000 | 3 000 000 |
| `kimi/kimi-k2.6` | 1 400 000 | 4 500 000 | 100 000 |
| `meta/llama-3.1` | 1 000 000 | 2 000 000 | 3 000 000 |
| `minimaxai/minimax-m2` | 1 000 000 | 2 000 000 | 3 000 000 |
| `minimaxai/minimax-m2.1` | 1 000 000 | 2 000 000 | 3 000 000 |
| `minimaxai/minimax-m2.5` | 1 000 000 | 2 000 000 | 3 000 000 |
| `minimaxai/minimax-m2.7` | 1 000 000 | 2 000 000 | 3 000 000 |
| `mistralai/devstral` | 1 000 000 | 2 000 000 | 3 000 000 |
| `mistralai/devstral-small-2` | 1 000 000 | 2 000 000 | 3 000 000 |
| `mistralai/mistral-large` | 80 000 | 2 500 000 | 20 000 |
| `mistralai/mistral-large-3:675b` | 1 000 000 | 2 000 000 | 3 000 000 |
| `mistralai/mistral-large-old` | 1 000 000 | 2 000 000 | 3 000 000 |
| `mistralai/mistral-medium` | 1 000 000 | 2 000 000 | 3 000 000 |
| `mistralai/mistral-small` | 1 000 000 | 2 000 000 | 3 000 000 |
| `nvidia/nemotron-3-super` | 1 000 000 | 2 000 000 | 3 000 000 |
| `nvidia/nv-embed-v1` | 1 000 000 | 2 000 000 | 3 000 000 |
| `openai/gpt-5.2` | 40 000 | 2 500 000 | 20 000 |
| `openai/gpt-5.2-codex` | 40 000 | 2 500 000 | 20 000 |
| `openai/gpt-5.3-codex` | 100 000 | 2 800 000 | 30 000 |
| `openai/gpt-5.4` | 1 000 000 | 4 000 000 | 30 000 |
| `openai/gpt-5.4-mini` | 12 000 | 1 200 000 | 10 000 |
| `openai/gpt-5.5` | 30 000 000 | 150 000 000 | 2 000 000 |
| `openai/gpt-oss:120b` | 1 000 000 | 2 000 000 | 3 000 000 |
| `openai/gpt-oss:20b` | 1 000 000 | 2 000 000 | 3 000 000 |
| `qwen/qwen3-coder:480b-a35b` | 1 000 000 | 2 000 000 | 3 000 000 |
| `qwen/qwen3-embedding:8b` | 1 000 000 | 2 000 000 | 3 000 000 |
| `qwen/qwen3-vl` | 40 000 | 2 500 000 | 20 000 |
| `qwen/qwen3.5-35b` | 500 | 5 000 | 50 |
| `qwen/qwen3.5-plus` | 60 000 | 2 500 000 | 20 000 |
| `qwen/qwen3.5:397b` | 8 000 | 80 000 | 800 |
| `qwen/qwen3.6-plus` | 100 000 | 3 000 000 | 30 000 |
| `wormsoft/agent/high` | 1 000 000 | 4 000 000 | 30 000 |
| `wormsoft/agent/low` | 500 | 5 000 | 50 |
| `wormsoft/agent/medium` | 30 000 | 1 000 000 | 50 |
| `wormsoft/code/high` | 80 000 | 3 000 000 | 30 000 |
| `wormsoft/code/low` | 500 | 5 000 | 50 |
| `wormsoft/code/medium` | 30 000 | 1 000 000 | 50 |
| `zai/glm-4.6` | 1 000 000 | 2 000 000 | 3 000 000 |
| `zai/glm-4.7` | 1 000 000 | 2 000 000 | 3 000 000 |
| `zai/glm-5` | 1 000 000 | 2 000 000 | 3 000 000 |
| `zai/glm-5.1` | 1 400 000 | 4 500 000 | 100 000 |

### Как считается usage

- Input tokens учитываются отдельно
- Cached input tokens учитываются отдельно
- Output tokens учитываются отдельно
- Списание выполняется на стороне backend после обработки результата

### Кредиты и подписки

- На этой странице цены уже переведены в кредиты
- Кредиты по подписке начисляются по расписанию в зависимости от выбранного тарифа
- Подробная таблица по лимитам и периодам доступна на странице подписок

---

## Подписки

> https://ai.wormsoft.ru/docs/llm/subscriptions

Текущие подписки, их стоимость и лимиты кредитов.

### Таблица подписок

| Подписка | Кредиты | Период | Стоимость |
|---|---|---|---|
| Free | 20 000 | каждые 10 часов | 0 ₽/мес |
| Promo | 150 000 | каждые 8 часов | 500 ₽/мес |
| Simple | 500 000 | каждые 5 часов | 1 500 ₽/мес |
| Payed | 3 000 000 | каждые 4 часа | 2 500 ₽/мес |
| Wormsoft developer | 5 000 000 | каждые 2 часа | 6 000 ₽/мес |
| Wormsoft boss | 10 000 000 | каждые 1 час | 12 000 ₽/мес |
