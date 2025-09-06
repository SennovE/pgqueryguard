# pgqueryguard

Установка venv:
```bash
pip install uv
make venv
```

---

Запуск CLI из src\pgqueryguard\cli.py:
```bash
uv run pgqueryguard ./file.sql
```

Для форматирования можно использовать pg_format, для этого надо указать флаг `--pg-format-file` и путь к конфигурации pg_format. Если запуск происходит с Windows, должен быть установлен Perl.

```bash
uv run pgqueryguard ./file.sql --pg-format-file ./path/to/pg_formatter
```

Для pg_format можно использовать конфиг, чтобы настроить форматирование. Для этого надо указать флаг --pg-format-cofig и путь к файлу.

Пример конфига:

```
keyword-case=2
type-case=1
spaces=2
no-extra-line=1
```

---
