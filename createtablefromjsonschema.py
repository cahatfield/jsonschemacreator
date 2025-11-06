import json

class JsonSchemaTableCreator:
    def __init__(self, jsonschema):
        self.schema = json.loads(jsonschema)
        self.table_name = self.schema.get("title")
        self.properties = self.schema.get("properties", {})
        self.required = set(self.schema.get("required", []))
        self.additional = self.schema.get("additionalProperties", {})

    def spark_type(self, jsontype):
        mapping = {
            "string": "STRING",
            "number": "DOUBLE",
            "integer": "INT",
            "boolean": "BOOLEAN"
        }
        return mapping.get(jsontype, "STRING")

    def column_defs(self):
        cols = []
        for col, prop in self.properties.items():
            coltype = self.spark_type(prop.get("type"))
            notnull = "NOT NULL" if col in self.required else ""
            cols.append(f"{col} {coltype} {notnull}".strip())
        return ",\n  ".join(cols)

    def constraints(self):
        cons = []
        pk = self.additional.get("primarykeycolumn")
        if pk:
            cons.append(f"CONSTRAINT {self.table_name}_pk PRIMARY KEY ({pk})")
        uniq = self.additional.get("uniquecolumns")
        if uniq:
            cons.append(f"CONSTRAINT {self.table_name}_unique UNIQUE ({', '.join(uniq)})")
        return ",\n  ".join(cons)

    def create_table_sql(self):
        cols = self.column_defs()
        cons = self.constraints()
        sql = f"""
CREATE TABLE {self.table_name} (
  {cols}{',' if cons else ''}
  {cons}
)
USING DELTA
"""
        return sql

    def create_table(self):
        sql = self.create_table_sql()
        return(sql)
        # spark.sql(sql)