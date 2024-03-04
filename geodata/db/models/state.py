from typing import Literal, Optional

from geodata.db.models.base import GeoZoneModel

StateTypes = Literal[
    None, 'province', 'city', 'territory', 'state', 'district', 'division',
    'municipality', 'special municipality', 'autonomous municipality', 'autonomous region',
    'special administrative region', 'capital district', 'department', 'county', 'region',
    'metropolitan department', 'European collectivity', 'metropolitan region', 'dependency',
    'metropolitan collectivity with special status', 'overseas region', 'overseas collectivity',
    'overseas territory', 'city with county rights', 'capital city', 'Union territory', 'governorate',
    'free municipal consortium', 'decentralized regional entity', 'federal district', 'prefecture',
    'capital territory', 'arctic region', 'capital', 'municipalities', 'autonomous community',
    'autonomous city in North Africa', 'SE-Z', 'canton', 'Region', 'republic', 'outlying area',
    'islands / groups of islands', 'island', 'federal dependency'
]


class State(GeoZoneModel):
    state_id_csc: int
    state_name: str
    state_code: Optional[str]
    state_type_csc: StateTypes
    state_id_wikidata: Optional[str] = None

    @property
    def id_csc(self) -> int:
        return self.state_id_csc

    @property
    def id_wikidata(self) -> str | None:
        """ `state_id_wikidata`."""
        return self.state_id_wikidata