from __future__ import annotations

from datetime import datetime
from typing import Any


def get_turno(timestamp_iso: str | None) -> str:
    if not timestamp_iso:
        return "desconhecido"
    try:
        dt = datetime.fromisoformat(timestamp_iso.replace("Z", "+00:00"))
    except ValueError:
        return "desconhecido"

    minutes = dt.hour * 60 + dt.minute
    if 320 <= minutes < 820:
        return "1_manha"
    if 820 <= minutes < 1320:
        return "2_tarde"
    return "3_noite"


def flatten_history(
    raw_data: dict[str, Any],
    modules_by_id: dict[int, dict[str, Any]],
    cells_by_id: dict[int, str],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for machine_type, events_by_day in raw_data.items():
        if not isinstance(events_by_day, list):
            continue

        for day_data in events_by_day:
            if not isinstance(day_data, dict):
                continue

            date_value = day_data.get("date", "")
            day_events = day_data.get("machines_events", [])
            if not isinstance(day_events, list):
                continue

            for item in day_events:
                event = item.get("event", {})
                machine = item.get("machine", {})
                losses = item.get("losses", [])
                event_id = event.get("id")
                if event_id is None:
                    continue

                module_id = machine.get("module_id")
                module = modules_by_id.get(module_id, {})
                cell_id = module.get("cell_id")
                interaction_time = event.get("interaction_time")
                created_at = event.get("created_at")
                timestamp = interaction_time or created_at
                first_loss = losses[0].get("loss_name") if losses else "SEM_PERDA"
                event_action = (event.get("action") or "").strip()
                event_description = (event.get("description") or "").strip()
                suggestion = (event.get("suggestion") or "").strip()
                interaction_description = (event.get("interaction_description") or "").strip()
                if event.get("type_event") == "SUGGESTION" and suggestion:
                    event_action = event_action or suggestion
                    event_description = event_description or suggestion
                else:
                    event_description = event_description or interaction_description

                event_date = date_value or event.get("date_operational") or ""

                records.append(
                    {
                        "event_id": int(event_id),
                        "machine_type": machine_type,
                        "date": event_date,
                        "turno": get_turno(timestamp),
                        "turno_data": f"{event_date}_{machine_type}_{get_turno(timestamp)}",
                        "action": event_action,
                        "description": event_description,
                        "type_event": event.get("type_event"),
                        "interaction": event.get("interaction"),
                        "interaction_time": interaction_time,
                        "created_at": created_at,
                        "loss_name": first_loss,
                        "machine_name": machine.get("machine_name"),
                        "machine_id": machine.get("machine_id"),
                        "module_id": module_id,
                        "module_name": module.get("name", f"Modulo {module_id}"),
                        "cell_id": cell_id,
                        "cell_name": cells_by_id.get(cell_id, f"Celula {cell_id}"),
                    }
                )

    return records
