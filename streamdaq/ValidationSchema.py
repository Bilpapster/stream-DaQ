from pydantic import BaseModel, create_model
import pathway as pw

class ValidationSchema:
    def __init__(self, name: str, fields: dict):
        """
        :param name: Name of the schema/model
        :param fields: Dictionary of field names to types, e.g. {"id": int, "name": str}
        """
        self.name = name
        self.fields = fields

        # Create Pydantic model dynamically
        self.pydantic_model = create_model(name, **fields)
        self.pathway_schema = pw.schema_from_types(**fields)

    def get_pydantic_model(self) -> BaseModel:
        """
        Create instance of Pydantic model.
        """
        return self.pydantic_model

    def get_pw_schema(self) -> pw.Schema:
        """
        Create instance of Pathway Schema.
        """
        return self.pathway_schema