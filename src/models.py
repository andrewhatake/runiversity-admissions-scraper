from dataclasses import dataclass, asdict


@dataclass
class CompetitionModel:
    url: str
    name: str
    faculty: str
    speciality: str
    type: str
    limit: int
    exs: list[str]

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

    def to_dict(self):
        return asdict(self)

COMP_COLS = ["name", "faculty", "speciality", "type"]