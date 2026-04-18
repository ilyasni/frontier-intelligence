# Books vs Lenses Audit

Этот файл помогает понять:
- нужно ли загружать книги в ChatGPT Project
- насколько текущие lenses покрывают книжную базу
- каких lenses пока не хватает

## Короткий вывод

Текущие lenses уже хорошо покрывают ядро роли `Designer AI Visionary`, но не покрывают весь книжный корпус.

Сейчас покрыто хорошо:
- продуктовая проверка идеи
- AI как системный сдвиг, а не косметика
- связь дизайна, бизнеса и продуктовой логики
- frontier scan и работа с сигналами
- memplex и защита концепции
- профессиональная позиция и честный feedback

Сейчас покрыто частично:
- disruptive innovation и jobs-to-be-done
- platform/network effects
- systems thinking
- forecasting и работа с неопределенностью
- AI governance, control и societal risk
- macro-tech trend framing

## Надо ли грузить книги в Project?

Короткий ответ: обычно нет.

Для ChatGPT Project не стоит загружать весь `books/` как рабочую базу по умолчанию, потому что:
- большинство файлов — это почти полные книги, а не сжатые reference-файлы
- они слишком большие и слишком шумные для повседневного retrieval
- GPT будет хуже держать приоритет на твоих lenses и проектном контексте
- часть книг полезна как фоновый корпус, но не как оперативный слой принятия решений

## Что лучше загружать в Project

Загружай в Project:
- текущие project docs
- strategy memos
- active research notes
- выбранные `lenses/`
- короткие synthesis-файлы по 1-3 страницы на книгу, если книга реально нужна этому проекту

Не загружай в Project по умолчанию:
- весь `books/`
- полные тексты книг только потому, что они важные

## Когда книгу все-таки можно загрузить

Загружай отдельную книгу только если:
- проект прямо опирается на ее framework
- ты хочешь много раз ссылаться именно на первоисточник
- у тебя нет короткого synthesis-файла

Даже в этом случае лучше сначала сделать краткий summary-файл и загружать его, а не полный текст.

## Покрытие: книга -> текущие lenses

### Agrawal, Gans, Goldfarb — От предвидения к власти

Покрытие:
- хорошо: `ai-product-lens.md`
- частично: `design-business-lens.md`

Чего не хватает:
- отдельного lens про AI economics
- логики "prediction -> decision -> workflow redesign"

### Andrew Chen — The Cold Start Problem

Покрытие:
- частично: `design-business-lens.md`
- частично: `product-lens.md`

Чего не хватает:
- отдельного `network-effects-lens`
- логики cold start, atomic network, liquidity, retention loops

### Clayton Christensen — Дилемма инноватора

Покрытие:
- частично: `frontier-workflow.md`
- частично: `design-business-lens.md`

Чего не хватает:
- отдельного `disruption-lens`
- различения sustaining vs disruptive moves

### Clayton Christensen — Закон успешных инноваций

Покрытие:
- хорошо: `product-lens.md`

Чего не хватает:
- явного слоя `jobs-to-be-done`

### Daniel Kahneman — Думай медленно, решай быстро

Покрытие:
- частично: `professional-stance-lens.md`
- частично: `frontier-workflow.md`

Чего не хватает:
- `decision-bias-lens`
- правил против overconfidence, halo effect, narrative fallacy

### Donella Meadows — Thinking in Systems

Покрытие:
- частично: `frontier-workflow.md`
- частично: `design-business-lens.md`

Чего не хватает:
- отдельного `systems-lens`
- stocks, flows, feedback loops, delays, leverage points

### Ethan Mollick — Co-Intelligence

Покрытие:
- хорошо: `ai-product-lens.md`

Чего не хватает:
- более прикладной lens про human-AI collaboration patterns

### Kevin Kelly — Неизбежно

Покрытие:
- частично: `frontier-workflow.md`

Чего не хватает:
- `macro-trends-lens`
- языка для долгих технологических сил, а не только локальных сигналов

### Marty Cagan — Inspired

Покрытие:
- хорошо: `product-lens.md`

Чего не хватает:
- почти ничего критичного для текущего ядра

### Mike Monteiro — Дизайн — это работа

Покрытие:
- хорошо: `professional-stance-lens.md`

Чего не хватает:
- почти ничего критичного для текущего ядра

### Mustafa Suleyman — Грядущая волна

Покрытие:
- частично: `ai-product-lens.md`
- частично: `design-business-lens.md`

Чего не хватает:
- `ai-governance-lens`
- язык про containment, asymmetry of power, deployment risk

### Platform Revolution

Покрытие:
- частично: `design-business-lens.md`

Чего не хватает:
- `platform-lens`
- multi-sided market logic, governance, value exchange, interaction design

### Philip Tetlock — Superforecasting

Покрытие:
- частично: `frontier-workflow.md`

Чего не хватает:
- `forecasting-lens`
- calibration, base rates, confidence discipline, update cadence

### Stuart Russell — Human Compatible

Покрытие:
- частично: `ai-product-lens.md`

Чего не хватает:
- `ai-alignment-lens`
- preference uncertainty, corrigibility, safe control

### Susan Blackmore — The Meme Machine

Покрытие:
- частично: `memplex-lens.md`

Чего не хватает:
- более точной меметической логики распространения, копируемости и отбора

### Teresa Torres — Continuous Discovery Habits

Покрытие:
- хорошо: `product-lens.md`

Чего не хватает:
- отдельного discovery-слоя, если хочешь сильнее заземлить vision в регулярную проверку

## Какие новые lenses реально стоит добавить

Если расширять пакет, я бы добавлял не все подряд, а только самые ценные:

1. `systems-lens.md`
Для Meadows и вообще для проверки обратных связей, задержек, вторичных эффектов.

2. `forecasting-lens.md`
Для Tetlock + Kahneman.
Поможет аккуратнее работать с уверенностью, прогнозами и обновлением гипотез.

3. `platform-network-lens.md`
Для Andrew Chen + Platform Revolution.
Нужен, если ты часто думаешь про network effects, ecosystems и interaction loops.

4. `disruption-jtbd-lens.md`
Для Christensen.
Нужен, если GPT должен отличать просто улучшение продукта от настоящего category shift.

5. `ai-governance-lens.md`
Для Russell + Suleyman.
Нужен, если проекты затрагивают доверие, контроль, безопасность и социальную цену AI.

## Практическая рекомендация

### Для GPT Knowledge

Оставь базой:
- все текущие `lenses/`

И только при необходимости добавь:
- короткие synthesis-файлы по 3-5 ключевым книгам

### Для Project Files

Загружай:
- project-specific документы
- только те book summaries, которые реально нужны этому проекту

### Для архива

Полные книги лучше оставить в репозитории как библиотеку, а не как default knowledge layer.
