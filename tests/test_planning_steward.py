from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PLANNING_STEWARD_SCRIPT = ROOT / "scripts" / "planning_steward.py"


def load_planning_steward_module():
    spec = importlib.util.spec_from_file_location(
        "planning_steward",
        PLANNING_STEWARD_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


planning_steward = load_planning_steward_module()


def test_collect_context_detects_repo_signals(tmp_path: Path) -> None:
    _write_minimal_repo(tmp_path)

    context = planning_steward.collect_context(tmp_path)

    assert context.has_operation_doc is True
    assert context.has_workflow_objective_impact is True
    assert context.has_backtest_metadata_migration is True
    assert context.has_backtest_metadata_code_writes is False
    assert context.has_objective_store is False
    assert context.next_migration_number == 5
    assert context.unchecked_plan_items == (
        "Backtest includes costs, baselines, regimes, and label-scramble",
    )


def test_propose_plan_returns_objective_packets_with_ticket_impacts(
    tmp_path: Path,
) -> None:
    _write_minimal_repo(tmp_path)

    proposal = planning_steward.propose_plan(root=tmp_path, max_objectives=3)

    objective_ids = [objective.objective_id for objective in proposal.objectives]
    assert objective_ids[:3] == [
        "wire-backtest-metadata-registry",
        "create-objective-store",
        "reconcile-phase-1-plan-state",
    ]
    first_ticket = proposal.objectives[0].expected_tickets[0]
    assert "persistence layer" in first_ticket.objective_impact
    assert first_ticket.technical_summary
    assert "db/migrations/" in first_ticket.do_not_touch


def test_markdown_output_is_user_readable_and_propose_only(tmp_path: Path) -> None:
    _write_minimal_repo(tmp_path)
    proposal = planning_steward.propose_plan(root=tmp_path, max_objectives=1)

    output = planning_steward.render_proposal(proposal, output_format="markdown")

    assert "# Planning Steward Proposal" in output
    assert "No Linear, GitHub, database, or vendor writes were performed." in output
    assert "Objective Impact:" in output
    assert "Migration Lane" in output
    assert "wire-backtest-metadata-registry" in output


def test_json_output_contains_stable_objective_shape(tmp_path: Path) -> None:
    _write_minimal_repo(tmp_path)
    proposal = planning_steward.propose_plan(root=tmp_path, max_objectives=1)

    payload = json.loads(
        planning_steward.render_proposal(proposal, output_format="json")
    )

    assert payload["status"] == "PROPOSE_ONLY"
    assert payload["objectives"][0]["objective_id"] == (
        "wire-backtest-metadata-registry"
    )
    assert "objective_impact" in payload["objectives"][0]["expected_tickets"][0]


def test_check_mode_prints_success_for_available_proposals(
    tmp_path: Path,
    capsys,
) -> None:
    _write_minimal_repo(tmp_path)

    exit_code = planning_steward.main(["--check", "--root", str(tmp_path)])

    assert exit_code == 0
    assert "OK: planning steward proposal check passed" in capsys.readouterr().out


def _write_minimal_repo(root: Path) -> None:
    (root / "docs" / "exec-plans" / "active").mkdir(parents=True)
    (root / "docs" / "exec-plans" / "active" / "phase-1-foundation.md").write_text(
        "\n".join(
            [
                "# Phase 1 Foundation Plan",
                "",
                "## Acceptance Criteria",
                "",
                "- [x] Trading calendar is seeded for 2014-2026",
                "- [ ] Backtest includes costs, baselines, regimes, and label-scramble",
            ]
        ),
        encoding="utf-8",
    )
    (root / "docs" / "Symphony-Operation.md").write_text(
        "# Silver Symphony Operation\n\nPlanning Steward\nObjective Impact\n",
        encoding="utf-8",
    )
    (root / "WORKFLOW.md").write_text(
        "## Proof Packet\n\n- Objective Impact\n",
        encoding="utf-8",
    )
    (root / "db" / "migrations").mkdir(parents=True)
    for filename in (
        "001_foundation.sql",
        "002_raw_objects_metadata.sql",
        "003_phase1_analytics.sql",
        "004_backtest_metadata.sql",
    ):
        (root / "db" / "migrations" / filename).write_text("-- test\n")
    (root / "src").mkdir()
    (root / "scripts").mkdir()
    (root / "scripts" / "planning_steward.py").write_text("# test\n")
