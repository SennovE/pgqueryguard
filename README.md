## CLI с проверками и формаированием для использования в pre-commit

[Документация](./checker/README.md)

## Приложение для анализа запросов

## Pre-commit-hook

Для использование проверки и форматирования перед коммитами в вашем репозитории надо создать файл .pre-commit-hooks.yaml с содержимым:

```
repos:
  - repo: https://github.com/SennovE/pgqueryguard
    rev: v0.1.0
    hooks:
      - id: pgqueryguard-check  # Проверка sql
      - id: pgqueryguard-report  # HTML отчет с подсказками для оптимизации 
        args: ["--db-url", "postgresql://user:pass@localhost:5432/postgres"]
```

Проверка будет запускаться во время исполнения команды git commit. В args передаются флаги. Не обязательно указывать все id в секции hooks.
