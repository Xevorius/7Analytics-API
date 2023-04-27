from pydantic import BaseModel

from Schemas.schemas_generic import Geometry


class MetStation(BaseModel):
    id: str
    name: str
    geometry: dict
    distance: float | None
    precip_list: list[tuple[int, float]] | None
    geometry_origin: Geometry | None
