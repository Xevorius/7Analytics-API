from pydantic import BaseModel


class Geometry(BaseModel):
    type: str
    coordinates: list[float]
    nearest: bool | None
