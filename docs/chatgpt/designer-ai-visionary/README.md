# Designer AI Visionary for ChatGPT

Этот пакет нужен, чтобы перенести текущий `.claude/skills/designer-ai-visionary`
в формат, удобный для ChatGPT `Projects` и при необходимости `GPTs` / `Skills`.

## Что внутри

- `project-instructions.md` — короткие инструкции для конкретного ChatGPT Project
- `gpt-instructions.md` — базовые reusable-инструкции для отдельного GPT или Skill
- `setup-guide.md` — как это лучше разложить в ChatGPT
- `lenses/` — reference-файлы, которые можно загрузить как knowledge/resources

## Рекомендуемая схема

- `GPT` или `Skill` = общая роль, режимы, стандарты ответа, anti-slop
- `Project` = текущий контекст, цель, рынок, ограничения, активные артефакты
- `Lenses` = стабильные рамки мышления и сжатые reference-файлы

## Минимальный запуск

1. Создай GPT или Skill `Designer AI Visionary`.
2. Вставь текст из `gpt-instructions.md`.
3. Загрузи файлы из `lenses/` как knowledge/resources.
4. Для каждого рабочего направления создай отдельный Project.
5. Вставь в Project текст из `project-instructions.md` и адаптируй блок контекста.

## Практический принцип

Не загружай целые книги без необходимости.
Сначала превращай их в короткие рабочие synthesis-файлы, а уже потом — в lens.
