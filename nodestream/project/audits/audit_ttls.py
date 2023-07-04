from itertools import chain
from typing import List

from ...model import TimeToLiveConfiguration
from ...pipeline.extractors.ttls import TimeToLiveConfigurationExtractor
from ..project import Project
from .audit import Audit


class AuditTimeToLiveConfigurations(Audit):
    name = "ttls"
    description = "Audit the project for missing TTLs"

    async def get_all_ttl_configurations(
        self, project: Project
    ) -> List[TimeToLiveConfiguration]:
        ttl_extractors = project.dig_for_step_of_type(TimeToLiveConfigurationExtractor)
        return [
            ttl
            for _, _, extractor in ttl_extractors
            async for ttl in extractor.extract_records()
        ]

    async def run(self, project: Project):
        schema = project.generate_graph_schema()
        unused_ttls = await self.get_all_ttl_configurations(project)
        for object_shape in chain(
            schema.known_node_types(), schema.known_relationship_types()
        ):
            for_shape = (ttl for ttl in unused_ttls if ttl.is_for_shape(object_shape))
            ttl = next(for_shape, None)
            if ttl is not None:
                unused_ttls.remove(ttl)
            else:
                err = f"Time to live not configured for {object_shape.graph_object_type} '{object_shape.object_type}'"
                self.failure(err)

        if unused_ttls:
            for ttl in unused_ttls:
                warn = f"Time to live configured for unknown object type '{ttl.object_type}'"
                self.warning(warn)

        if self.failure_count == 0:
            self.success("All Object Types Have TTLs Configured")
