from pydantic import BaseModel


class MetStation(BaseModel):
    id: str
    name: str
    geometry: str
    distance: float | None
    precipitation: list[float] | None
    geometry_origin: str | None

    class Config:
        arbitrary_types_allowed = True


class MetFeature(BaseModel):
    feature: list


class MetForcast(BaseModel):
    shape: list[MetFeature]
