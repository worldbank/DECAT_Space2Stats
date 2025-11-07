# Space2Stats Development Database

.PHONY: help up down logs reset verify generate clean apply-schema

help:
	@echo "Space2Stats Development Database Commands:"
	@echo "  make up        - Start database with sample data"
	@echo "  make down      - Stop database"
	@echo "  make logs      - Show database logs"
	@echo "  make reset     - Reset database (removes all data)"
	@echo "  make clean     - Stop and remove everything"

up:
	@echo "Starting development database..."
	docker-compose up -d
	@echo "Waiting for database to be ready..."
	@$(MAKE) wait
	@echo "Database ready at localhost:5439"
	@$(MAKE) wait
	@$(MAKE) seed

down:
	docker-compose down

logs:
	docker-compose logs -f database

reset:
	@echo "Resetting database (this removes all data)..."
	@$(MAKE) clean
	@$(MAKE) up
	@$(MAKE) wait

apply-schema:
	@echo "Applying database schema updates..."
	docker-compose exec -T database psql -U username -d postgres -f /docker-entrypoint-initdb.d/01-init.sql

seed:
	@python seed_sample_data.py

clean:
	docker-compose down -v --remove-orphans
	@echo "Removing local database files..."
	@rm -rf .pgdata

# Cross-platform wait command
wait:
ifeq ($(OS),Windows_NT)
	@powershell -Command "Start-Sleep -Seconds 5"
else
	@sleep 5
endif

# Cross-platform Python execution
run-python:
ifeq ($(OS),Windows_NT)
	@python $(filter-out run-python,$(MAKECMDGOALS))
else
	@python $(filter-out run-python,$(MAKECMDGOALS))
endif
