.PHONY: warm up down logs verify-stack reset corrupt-users inject-duplicates test-fast smoke-test dashboard check \
        hint-phase1-1 hint-phase1-2 hint-phase1-3 \
        hint-phase2-1 hint-phase2-2 hint-phase2-3 \
        hint-phase3-1 hint-phase3-2 hint-phase3-3

warm:
	docker compose pull --ignore-pull-failures
	docker compose build
	@echo "✅ Images pulled and built. Run 'make up' to start the stack."

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs --tail=200 -f

verify-stack:
	@bash scripts/verify_stack.sh

# The targets below are stubs for now; they get wired up in later tasks.
reset:
	docker compose exec -T airflow-scheduler python /opt/chaos/reset.py

corrupt-users:
	docker compose exec -T airflow-scheduler python /opt/chaos/corrupt_users.py

inject-duplicates:
	docker compose exec -T airflow-scheduler python /opt/chaos/inject_duplicates.py

test-fast:
	docker compose exec -T airflow-scheduler bash -c 'cd /opt/airflow && pytest tests/ -q --deselect tests/test_end_to_end.py'

smoke-test:
	docker compose exec -T airflow-scheduler bash -c 'cd /opt/airflow && pytest tests/test_end_to_end.py -v -m integration'

dashboard:
	docker compose exec airflow-scheduler python /opt/airflow/dashboard/live_status.py

# ─── Self-check + per-phase hints (T15) ─────────────────────────────────────
check:
	@./check.sh

hint-phase1-1:; @cat .hints/phase1-1.md
hint-phase1-2:; @cat .hints/phase1-2.md
hint-phase1-3:; @cat .hints/phase1-3.md
hint-phase2-1:; @cat .hints/phase2-1.md
hint-phase2-2:; @cat .hints/phase2-2.md
hint-phase2-3:; @cat .hints/phase2-3.md
hint-phase3-1:; @cat .hints/phase3-1.md
hint-phase3-2:; @cat .hints/phase3-2.md
hint-phase3-3:; @cat .hints/phase3-3.md
