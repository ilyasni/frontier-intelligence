# Book Summaries

Это короткие synthesis-файлы по ключевым книгам.
Они нужны как knowledge-ready слой для ChatGPT вместо загрузки полных текстов.

## Зачем они нужны

- дают меньше шума, чем полные книги
- быстрее извлекаются моделью
- лучше сочетаются с `lenses/`
- удобнее загружать и в GPT Knowledge, и в конкретный Project

## Как использовать

- В GPT Knowledge загружай summaries только если тема часто повторяется.
- В Project Files загружай только те summaries, которые прямо нужны текущему направлению.
- Полные книги оставляй в `books/` как библиотеку, не как default layer.

## Рекомендуемый стартовый набор

- `systems-thinking-summary.md`
- `forecasting-summary.md`
- `disruption-jtbd-summary.md`

Опционально:
- `platform-network-summary.md`
- `ai-governance-summary.md`
