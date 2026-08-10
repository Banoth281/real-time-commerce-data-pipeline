.PHONY: start stop logs test reset

start:
	cp .env.example .env 2>/dev/null || true
	docker compose up --build -d

stop:
	docker compose down

logs:
	docker compose logs -f producer processor api

test:
	python -m pytest -q

reset:
	docker compose down -v

