from pydantic import BaseModel


class CulvertResults(BaseModel):
    id: str
    timestamp: list[int]
    values: list[float]
    images: list[dict[str, str | int]] | None