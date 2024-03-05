from typing import Optional

from pydantic import BaseModel, ConfigDict


class BaseSearch(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: Optional[str] = None
    search_limit: int = 32

    def search(self, row):
        mention_name, annotated_text, annotated_idx = row['name'], row['text'], row['id']

        results = [
            {'wikidata_id': "Q0000", 'name': mention_name}
        ]
        return results

    @property
    def method(self):
        return self.__class__.__name__

    @property
    def source(self):
        return self.name or self.cache.__class__.__name__
