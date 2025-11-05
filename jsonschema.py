import os
import json

class schema:
    def __init__(self, name, location):
        self.schemaname = name 
        self.schemalocation = f'./{location}'

    def create(self):
        """Create the directory if it doesn't exist"""
        jsonschemafolder = f"{self.schemalocation}"
        jsonschemafile = f"{jsonschemafolder}/{self.schemaname}.schema.json"

        if not os.path.exists(jsonschemafolder):
            os.makedirs(jsonschemafolder)
            # print(f"Created directory: {jsonschemafolder}")

        if not os.path.exists(jsonschemafile):
            with open(jsonschemafile, "w") as f:
                json.dump({
                    "title": self.schemaname,
                    "description": "",
                    "type": "object",
                    "properties": {}
                }, f)
                # print(f"Created file: {jsonschemafile}")

    def description(self, text):
        """Return a description instance with access to parent schema"""
        return self.Description(self, text)

    def properties(self, name, description=None, type=None):
        """Return a properties instance with access to parent schema"""
        return self.Properties(self, name, description, type)
    
    def required(self, property_names):
        """Return a required instance with access to parent schema"""
        return self.Required(self, property_names)
    
    def additionalproperties(self, allowed):
        """Return an additionalProperties instance with access to parent schema"""
        return self.AdditionalProperties(self, allowed)

    class Description:
        def __init__(self, parent_schema, text):
            self.parent_schema = parent_schema
            self.text = text

        def modify(self):
            """Update the description field in the schema file"""
            jsonschemafile = f"{self.parent_schema.schemalocation}/{self.parent_schema.schemaname}.schema.json"

            if os.path.exists(jsonschemafile):
                # Read existing schema
                with open(jsonschemafile, 'r') as f:
                    schema_content = json.load(f)
                
                # Update description
                schema_content["description"] = self.text
                
                # Write back to file
                with open(jsonschemafile, 'w') as f:
                    json.dump(schema_content, f, indent=2)
                
                # print(f"Updated description in {jsonschemafile}")
            else:
                print(f"Schema file not found: {jsonschemafile}")

    class Properties:
        def __init__(self, parent_schema, name, description=None, type=None):
            self.parent_schema = parent_schema
            self.name = name
            self.description = description
            self.type = type

        def add(self):
            """Add a property to the schema (prevents duplicate property names)"""
            if self.description is None or self.type is None:
                print("Error: Both description and type are required for adding a property")
                return
                
            jsonschemafile = f"{self.parent_schema.schemalocation}/{self.parent_schema.schemaname}.schema.json"

            if os.path.exists(jsonschemafile):
                # Read existing schema
                with open(jsonschemafile, 'r') as f:
                    schema_content = json.load(f)
                
                # Ensure properties object exists
                if "properties" not in schema_content:
                    schema_content["properties"] = {}
                
                # Check if property already exists
                if self.name in schema_content["properties"]:
                    print(f"Error: Property '{self.name}' already exists in the schema. Cannot add duplicate property names.")
                    return
                
                # Add new property
                schema_content["properties"][self.name] = {
                    "description": self.description,
                    "type": self.type
                }
                
                # Write back to file
                with open(jsonschemafile, 'w') as f:
                    json.dump(schema_content, f, indent=2)
                
                # print(f"Added property '{self.name}' to {jsonschemafile}")
            else:
                print(f"Schema file not found: {jsonschemafile}")
        
        def remove(self):
            """Remove a property from the schema"""
            jsonschemafile = f"{self.parent_schema.schemalocation}/{self.parent_schema.schemaname}.schema.json"

            if os.path.exists(jsonschemafile):
                # Read existing schema
                with open(jsonschemafile, 'r') as f:
                    schema_content = json.load(f)
                
                # Remove property if it exists
                if "properties" in schema_content and self.name in schema_content["properties"]:
                    del schema_content["properties"][self.name]
                
                # Write back to file
                with open(jsonschemafile, 'w') as f:
                    json.dump(schema_content, f, indent=2)
                
                # print(f"Removed property '{self.name}' from {jsonschemafile}")
            else:
                print(f"Schema file not found: {jsonschemafile}")

    class Required:
        def __init__(self, parent_schema, property_names):
            self.parent_schema = parent_schema
            self.property_names = property_names

        def add(self):
            """Add required properties to the schema"""
            jsonschemafile = f"{self.parent_schema.schemalocation}/{self.parent_schema.schemaname}.schema.json"

            if os.path.exists(jsonschemafile):
                # Read existing schema
                with open(jsonschemafile, 'r') as f:
                    schema_content = json.load(f)
                
                # Ensure required array exists
                if "required" not in schema_content:
                    schema_content["required"] = []
                
                # Add required properties
                for prop in self.property_names:
                    if prop not in schema_content["required"]:
                        schema_content["required"].append(prop)
                
                # Write back to file
                with open(jsonschemafile, 'w') as f:
                    json.dump(schema_content, f, indent=2)
                
                # print(f"Added required properties to {jsonschemafile}")
            else:
                print(f"Schema file not found: {jsonschemafile}")

        def remove(self):
            """Remove required properties from the schema"""
            jsonschemafile = f"{self.parent_schema.schemalocation}/{self.parent_schema.schemaname}.schema.json"

            if os.path.exists(jsonschemafile):
                # Read existing schema
                with open(jsonschemafile, 'r') as f:
                    schema_content = json.load(f)
                
                # Remove required properties
                if "required" in schema_content:
                    schema_content["required"] = [
                        prop for prop in schema_content["required"]
                        if prop not in self.property_names
                    ]
                
                # Write back to file
                with open(jsonschemafile, 'w') as f:
                    json.dump(schema_content, f, indent=2)
                
                # print(f"Removed required properties from {jsonschemafile}")
            else:
                print(f"Schema file not found: {jsonschemafile}")

    class AdditionalProperties:
        def __init__(self, parent_schema, allowed):
            self.parent_schema = parent_schema
            self.allowed = allowed

        def modify(self):
            """Modify additionalProperties in the schema"""
            jsonschemafile = f"{self.parent_schema.schemalocation}/{self.parent_schema.schemaname}.schema.json"

            if os.path.exists(jsonschemafile):
                # Read existing schema
                with open(jsonschemafile, 'r') as f:
                    schema_content = json.load(f)
                

                # Ensure additionalProperties exists
                if "additionalProperties" not in schema_content:
                    schema_content["additionalProperties"] = {}

                # Modify additionalProperties
                # schema_content["additionalProperties"] = self.allowed

                for prop,value in self.allowed.items():
                    if prop not in schema_content["additionalProperties"]:
                        schema_content["additionalProperties"][prop] = value

                # Write back to file
                with open(jsonschemafile, 'w') as f:
                    json.dump(schema_content, f, indent=2)
                
                # print(f"Modified additionalProperties in {jsonschemafile}")
            else:
                print(f"Schema file not found: {jsonschemafile}")

        def remove(self):
            """Remove additionalProperties from the schema"""
            jsonschemafile = f"{self.parent_schema.schemalocation}/{self.parent_schema.schemaname}.schema.json"

            if os.path.exists(jsonschemafile):
                # Read existing schema
                with open(jsonschemafile, 'r') as f:
                    schema_content = json.load(f)
                
                # Remove additionalProperties
                if "additionalProperties" in schema_content:
                    del schema_content["additionalProperties"]
                
                # Write back to file
                with open(jsonschemafile, 'w') as f:
                    json.dump(schema_content, f, indent=2)
                
                # print(f"Removed additionalProperties from {jsonschemafile}")
            else:
                print(f"Schema file not found: {jsonschemafile}")
