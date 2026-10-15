from datetime import date
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

# ============================================================
# PROJECT
# ============================================================

class ProjectCreate(BaseModel):
    name: str

    parent_project_id: UUID | None = None

    area: str = ""
    responsible_team: str = ""

    status: Literal[
        "approved",
        "development",
        "on_hold",
        "hyper_care",
        "completed",
    ]

    project_manager_id: UUID
    spoc_user_id: UUID | None = None

    scope: str = ""
    objective: str = ""

    start_date: date | None = None
    due_date: date | None = None
    actual_end_date: date | None = None

    progress: float = 0

    quarter: str = ""
    qevo: str = ""
    solution_type: str = ""

    complexity: Literal[
        "low",
        "medium",
        "high",
        "very_high",
    ]

    health: Literal[
        "healthy",
        "attention",
        "critical",
    ]

    benefit_financial_year: float = 0
    benefit_hours_year: float = 0
    benefit_paper_year: float = 0
    benefit_indirect_cost_year: float = 0

    benefit_comments: str = ""

    documents_url: list[str] = Field(
        default_factory=list
    )

    completion_animation_shown: bool = False

    created_by: UUID

class ProjectUpdate(BaseModel):
    name: str | None = None

    parent_project_id: UUID | None = None

    area: str | None = None
    responsible_team: str | None = None

    status: Literal[
        "approved",
        "development",
        "on_hold",
        "hyper_care",
        "completed",
    ] | None = None

    project_manager_id: UUID | None = None
    spoc_user_id: UUID | None = None

    scope: str | None = None
    objective: str | None = None

    start_date: date | None = None
    due_date: date | None = None
    actual_end_date: date | None = None

    progress: float | None = None

    quarter: str | None = None
    qevo: str | None = None
    solution_type: str | None = None

    complexity: Literal[
        "low",
        "medium",
        "high",
        "very_high",
    ] | None = None

    health: Literal[
        "healthy",
        "attention",
        "critical",
    ] | None = None

    benefit_financial_year: float | None = None
    benefit_hours_year: float | None = None
    benefit_paper_year: float | None = None
    benefit_indirect_cost_year: float | None = None

    benefit_comments: str | None = None

    documents_url: list[str] | None = None

    completion_animation_shown: bool | None = None

# ============================================================
# ACTIVITY
# ============================================================

class ActivityCreate(BaseModel):
    project_id: UUID

    name: str

    description: str = ""
    task_group: str = ""

    assigned_to_user_id: UUID

    start_date: date | None = None
    actual_start_date: date | None = None
    due_date: date | None = None
    actual_end_date: date | None = None

    status: Literal[
        "not_started",
        "in_progress",
        "completed",
        "overdue",
    ]

    priority: Literal[
        "low",
        "medium",
        "high",
        "critical",
    ]

    predecessor_activity_id: UUID | None = None

    planned_duration: int = 0
    actual_duration: int | None = None

    progress: float = 0

    updates: str = ""

    created_by: UUID


class ActivityUpdate(BaseModel):
    project_id: UUID | None = None

    name: str | None = None

    description: str | None = None
    task_group: str | None = None

    assigned_to_user_id: UUID | None = None

    start_date: date | None = None
    actual_start_date: date | None = None
    due_date: date | None = None
    actual_end_date: date | None = None

    status: Literal[
        "not_started",
        "in_progress",
        "completed",
        "overdue",
    ] | None = None

    priority: Literal[
        "low",
        "medium",
        "high",
        "critical",
    ] | None = None

    predecessor_activity_id: UUID | None = None

    planned_duration: int | None = None
    actual_duration: int | None = None

    progress: float | None = None

    updates: str | None = None

# ============================================================
# PROJECT MEMBER
# ============================================================

class ProjectMemberCreate(BaseModel):
    project_id: UUID
    user_id: UUID
    project_role: str


# ============================================================
# COMMENT
# ============================================================

class ProjectCommentCreate(BaseModel):
    project_id: UUID
    user_id: UUID
    comment: str


# ============================================================
# SUCCESS METRIC
# ============================================================

class ProjectSuccessMetricCreate(BaseModel):
    project_id: UUID

    name: str

    benefit_description: str = ""

    planned_value: float = 0
    actual_value: float = 0

    level: str = ""


class ProjectSuccessMetricUpdate(BaseModel):
    name: str | None = None

    benefit_description: str | None = None

    planned_value: float | None = None
    actual_value: float | None = None

    level: str | None = None