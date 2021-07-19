
from uuid import uuid4


class NoGrouper:
    """
    NoGrouper is the Grouper type meant to be used when groups are determined at extraction time.

    For instance, if we are using a sampler via a funcX function to determine first extractor,
    then we will use that to determine the first group.
    """

    def __init__(self, logger):
        self.name = "file_is_group"  # TODO: add to parent class
        self.logger = logger
        self.total_graphing_time = 0

    def group(self, file_ls):
        families = []

        for file_obj in file_ls:
            family_uuid = str(uuid4())
            groups = [{"files": [file_obj], "parser": None}]

            family = {"family_id": family_uuid, "files": [file_obj], "groups": groups, "extractor": "sampler"}
            families.append(family)

        return families
