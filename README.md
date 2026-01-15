## Создание виртуального окружения

#MacOS
```bash
python3.12 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install -r requirements.txt
```

#Windows
```bash
python -m venv venv                     ## Создание виртуального окружения
venv/Scripts/Activate.ps1               ## Активация виртуального окружения
python.exe -m pip install --upgrade pip ## обновление менеджера пакетов
pip install -r requirements.txt         ## установка зависимостей
```

## Разворачивание инфраструктуры

```bash
docker-compose up -d
```