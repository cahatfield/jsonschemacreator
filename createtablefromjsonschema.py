import json

# jsonschema = """
# {
#     "title": "products",
#     "description": "This is a jsonschema file defining the table for validating a product record",
#     "type": "object",
#     "properties": {
#         "productkey": {
#             "description": "The unique identifier for the product",
#             "type": "number"
#         },
#         "vendorname": {
#             "description": "The name of the vendor",
#             "type": "string"
#         },
#         "catalogid": {
#             "description": "the id assigned by a vendor in a catalog",
#             "type": "string"
#         },
#         "productdescription": {
#             "description": "The description of the product",
#             "type": "string"
#         }
#     },
#     "required": [
#         "vendorname",
#         "catalogid",
#         "productdescription"
#     ],
#     "additionalProperties": {
#         "primarykeycolumn": "productkey",
#         "uniquecolumns": [
#             "vendorname",
#             "catalogid",
#             "productdescription"
#         ]
#     },
#     "x-tags": {
#         "testkey": "testvalue",
#         "testkey2": "testvalue2"
#     }
# }
# """

class JsonSchemaTableCreator:
    def __init__(self, jsonschema):
        self.jsonschema = json.loads(jsonschema)
        self.table_name = self.jsonschema.get("title")
        self.properties = self.jsonschema.get("properties", {})
        self.required = set(self.jsonschema.get("required", []))
        self.additional = self.jsonschema.get("additionalProperties", {})
        self.tags = self.jsonschema.get("x-tags", {})
        self.catalog = self.jsonschema.get("x-catalog")
        self.schema = self.jsonschema.get("x-schema")

    def spark_type(self, jsontype):
        mapping = {
            "string": "STRING",
            "number": "DOUBLE",
            "integer": "INT",
            "biginterger": "BIGINT",
            "boolean": "BOOLEAN"
        }
        return mapping.get(jsontype, "STRING")

    def column_defs(self):
        cols = []
        for col, prop in self.properties.items():
            coltype = self.spark_type(prop.get("type"))
            notnull = "NOT NULL" if col in self.required else ""
            if cols ==[]:
                cols.append(f"{col} {coltype} {notnull}".strip())
            else:
                cols.append(f",{col} {coltype} {notnull}".strip())

            pk = self.additional.get("primarykeycolumn")
            # print(pk, col)
            if col == pk:
                cols.append(' GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)')
        return "\n  ".join(cols)
    
    def constraints(self):
        cons = []
        pk = self.additional.get("primarykeycolumn")
        if pk:
            cons.append(f",CONSTRAINT {self.table_name}_pk PRIMARY KEY ({pk})")
        uniq = self.additional.get("uniquecolumns")
        if uniq:
            cons.append(f"--,CONSTRAINT {self.table_name}_unique UNIQUE ({', '.join(uniq)})")
        return "\n  ".join(cons)

    # def primarykey(self):
    #     pk_stmts = []
    #     pk = self.additional.get("primarykeycolumn")
    #     if pk:
    #         pk_stmts.append(f"ALTER TABLE {self.table_name} ALTER COLUMN {pk} GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1);")
    #     return "\n".join(pk_stmts)

    def tag_sql(self):
        if not self.tags:
            return ""
        tag_stmts = []
        for k, v in self.tags.items():
            tag_stmts.append(f"ALTER TABLE {self.catalog}.{self.schema}.{self.table_name} SET TAGS ('{k}' = '{v}');")
        return "\n".join(tag_stmts)

    def create_table_sql(self):
        cols = self.column_defs()
        cons = self.constraints()
        # pkey = self.primarykey()
        tag_sql = self.tag_sql()
        sql = f"""
DROP TABLE IF EXISTS {self.catalog}.{self.schema}.{self.table_name};

CREATE TABLE {self.catalog}.{self.schema}.{self.table_name} (
  {cols}
  {cons}
)
USING DELTA;
"""
# sql = f"""
# CREATE TABLE {self.table_name} (
#   {cols}{',' if cons else ''}
#   {cons}
# )
# USING DELTA;
# """
        # sql = sql + ("\n" + pkey if pkey else "")
        sql = sql + ("\n" + tag_sql if tag_sql else "")
        return(sql)

    def create_table(self):
        sql = self.create_table_sql()
        return(sql)
        # spark.sql(sql)

# creator = JsonSchemaTableCreator(jsonschema)
# table_sql = creator.create_table()
# print(table_sql)