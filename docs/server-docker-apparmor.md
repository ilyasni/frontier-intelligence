# Docker build и AppArmor на хосте (runc: unable to apply apparmor profile)

Симптом при `docker compose build` / `docker build`:

```text
apparmor failed to apply profile: write ... /proc/thread-self/attr/apparmor/exec: no such file or directory
```

Ниже — варианты от **менее инвазивных** к более радикальным. Выбери один подход под свою политику безопасности.

Ещё симптом при использовании **`docker-compose.build-host-fix.yml`** (build `privileged: true`):

```text
granting entitlement security.insecure is not allowed by build daemon configuration
```

Значит BuildKit на хосте **не разрешает** insecure-entitlement — используй **п. 1** (legacy builder) или настройку демона, а не override с `privileged`.

---

## 0. Healthcheck «unhealthy» при живом сервисе

Если `docker compose ps` показывает **unhealthy** у postgres/redis/qdrant сразу после старта, часто не хватает **`start_period`** у healthcheck (инициализация БД, загрузка Qdrant). В [`docker-compose.yml`](../docker-compose.yml) для этих сервисов заданы `start_period`, чтобы не путать кратковременный старт с сбоем.

---

## 1. Legacy builder: `DOCKER_BUILDKIT=0` (часто достаточно для AppArmor)

Не требует `privileged` и override-файла:

```bash
cd /opt/frontier-intelligence
export DOCKER_BUILDKIT=0
docker compose --profile core --profile ingest build ingest
docker compose --profile core --profile ingest up -d --force-recreate ingest
```

Скрипт: **[`scripts/server-build-ingest-fix.sh`](../scripts/server-build-ingest-fix.sh)**.

Сборка **worker** (сервис в профиле `worker`, зависит от `postgres` из `core` — указывай оба профиля):

```bash
cd /opt/frontier-intelligence
export DOCKER_BUILDKIT=0
docker compose --profile core --profile worker build worker
docker compose --profile core --profile worker up -d --force-recreate gpt2giga-proxy worker
```

Если `docker compose exec redis redis-cli …` падает с **AppArmor** на том же хосте, проверяй стримы через контейнер с `--network container:…redis…`:

```bash
docker run --rm --network container:frontier-intelligence-redis-1 redis:7-alpine \
  redis-cli XLEN stream:posts:parsed
```

Универсальная обёртка (по умолчанию тоже `DOCKER_BUILDKIT=0`): **[`scripts/server-compose-build-with-fix.sh`](../scripts/server-compose-build-with-fix.sh)**.  
Чтобы снова подключить override с `privileged`, задай **`USE_COMPOSE_BUILD_PRIVILEGED=1`** (нужен разрешённый на демоне `security.insecure`).

---

## 2. Сборка с привилегированным build-контейнером (BuildKit + entitlement)

Override **[`docker-compose.build-host-fix.yml`](../docker-compose.build-host-fix.yml)** — `privileged: true` **только на этапе сборки**. Требуется, чтобы build daemon разрешал **security.insecure** (см. документацию Docker/BuildKit для твоей ОС).

```bash
cd /opt/frontier-intelligence

docker compose \
  -f docker-compose.yml \
  -f docker-compose.build-host-fix.yml \
  --profile core \
  --profile ingest \
  build ingest

docker compose \
  -f docker-compose.yml \
  -f docker-compose.build-host-fix.yml \
  --profile core \
  --profile ingest \
  up -d --force-recreate ingest
```

---

## 3. Разовая сборка через `docker build` с security-opt

Если не хочешь править compose:

```bash
cd /opt/frontier-intelligence
DOCKER_BUILDKIT=1 docker build \
  --security-opt apparmor=unconfined \
  -f ingest/Dockerfile \
  -t frontier-intelligence-ingest \
  .
```

Дальше нужно, чтобы `docker compose up` подхватил тег (в основном compose у `ingest` нет явного `image:` — тогда Compose сам задаёт имя образа по проекту). Проще согласовать с `docker compose build` из пункта 1.

---

## 4. Проверка и правка AppArmor на хосте (без отключения глобально)

```bash
sudo aa-status
sudo systemctl status apparmor
```

Перезагрузка профилей и демона (Debian/Ubuntu):

```bash
sudo systemctl reload apparmor
sudo systemctl restart docker
```

Если недавно обновляли **kernel** или **runc/containerd**, перезагрузка хоста часто убирает рассинхрон.

Убедись, что профиль `docker-default` загружен:

```bash
sudo aa-status | grep -i docker
```

---

## 5. Отключение AppArmor только для демона Docker (осторожно)

Только если пункты 1–3 (сборка) не помогают и хост не multi-tenant.

Создай drop-in для `docker.service`:

```bash
sudo mkdir -p /etc/systemd/system/docker.service.d/
sudo tee /etc/systemd/system/docker.service.d/no-apparmor.conf <<'EOF'
[Service]
ExecStart=
ExecStart=/usr/bin/dockerd -H fd:// --containerd=/run/containerd/containerd.sock --default-security-opt apparmor=unconfined
EOF
```

**Важно:** строка `ExecStart=` обнуляет наследуемый `ExecStart`; путь к `dockerd` и остальные флаги должны совпасть с тем, что был в оригинальном юните. Проверь:

```bash
systemctl cat docker.service
```

После правки:

```bash
sudo systemctl daemon-reload
sudo systemctl restart docker
```

Откат: удали файл в `docker.service.d/` и снова `daemon-reload` + `restart docker`.

---

## 6. Параметр ядра `apparmor=0` (ядерный вариант)

Только для отладочных машин. В GRUB добавь в командную строку ядра `apparmor=0`, обнови конфиг загрузчика и перезагрузись. **Не рекомендуется** для продакшена.

---

## Ссылки

- [AppArmor и Docker (официально)](https://docs.docker.com/engine/security/apparmor/)
- [Compose: секция `build`, поле `privileged`](https://docs.docker.com/reference/compose-file/build/#privileged)
