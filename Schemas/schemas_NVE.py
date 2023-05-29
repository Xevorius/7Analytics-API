from pydantic import BaseModel


class NveStation(BaseModel):
    id: str
    name: str
    geometry: str
    distance: float | None
    precipitation: list[float] | None
    geometry_origin: str | None

    class Config:
        arbitrary_types_allowed = True


class NveCulvert(BaseModel):
    id: str
    name: str
    geometry: str
    distance: float | None
    precipitation: list[float] | None
    geometry_origin: str | None

    class Config:
        arbitrary_types_allowed = True
