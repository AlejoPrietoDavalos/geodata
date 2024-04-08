from typing import Literal, Optional, List

from pydantic import Field

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
    state_name_native: Optional[str] = None
    state_name_english: Optional[str] = None
    state_code: Optional[str] = None
    state_type_csc: StateTypes
    state_id_wikidata: Optional[str] = None
    postal_codes_wikipedia: List[str] = Field(default_factory=list)
    postal_codes_wikipedia_clean: List[str] = Field(default_factory=list)

    @property
    def name(self) -> str:
        return self.state_name
    
    @property
    def name_native(self) -> str | None:
        return self.state_name_native

    @property
    def name_english(self) -> str | None:
        """ English name of the entity."""
        return self.state_name_english

    @property
    def id_csc(self) -> int:
        return self.state_id_csc

    @property
    def id_wikidata(self) -> str | None:
        """ `state_id_wikidata`."""
        return self.state_id_wikidata