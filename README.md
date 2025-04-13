# Local FTS Engine: PostgreSQL + Python

Этот проект использует **PostgreSQL Full-Text Search** и **Python** для создания системы поиска по локальным файлам (`.pdf`, `.txt`, `.md`).

## Bозможности

*   **Индексация:** Автоматический парсинг и индексация контента указанных директорий.
*   **FTS:** Многоязычный поиск (RU/EN) с морфологией (`stemming`) и нечувствительностью к диакритике (`unaccent`).
*   **Релевантность:** Ранжирование (`ts_rank_cd`) и подсветка (`ts_headline`) результатов.
*   **Устойчивость:** Базовая поддержка нечеткого поиска через `pg_trgm`.

### Инструкция по запуску

**Требования:** Python 3.8+, PostgreSQL 12+, Git, pip.

1.  **Клонировать репозиторий:**
    ```bash
    git clone https://github.com/diplomabsu/localdocs.git && cd localdocs
    ```

2.  **Установить зависимости:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Настроить PostgreSQL:**
    *   Создайте БД (напр., `pkm_search_db`) и пользователя (напр., `pkm_user`) с паролем и правами.
    *   В созданной БД выполните:
        ```sql
        CREATE EXTENSION IF NOT EXISTS unaccent;
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
        ```

4.  **Настроить подключение:**
    *   Создайте файл `.env` в корне проекта.
    *   Заполните его вашими данными (DB\_NAME, DB\_USER, DB\_PASSWORD, DB\_HOST, DB\_PORT), используя имя БД и пользователя из шага 3.

5.  **Инициализировать схему FTS:**
    ```python
    python search_pkm.py --setup
    ```

6.  **Проиндексировать документы:**
    ```bash
    python parse.py /путь/к/вашим/документам
    ```

7.  **Запустить поиск:**
    *   **Интерактивно:**
        ```bash
        python search_pkm.py
        ```
    *   **С параметрами:**
        ```bash
        python search_pkm.py -q "запрос" [-l язык] [-n лимит]
        ```
