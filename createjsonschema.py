import os
import json

class schema:
    def __init__(self, name, textstring):
        self.schemaname = name 
        self.textstring = textstring

    def create(self):
        # self.textstring = []
        if self.textstring == '':
            self.textstring = self.textstring + json.dumps({
                "title": self.schemaname,
                "description": "",
                "type": "object",
                "properties": {}   
            })

        return(self.textstring)
    
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

            if self.parent_schema.textstring != '':
                # Read existing schema
                schema_content = json.loads(self.parent_schema.textstring)
                
                # Update description
                schema_content["description"] = self.text
                
                # Write back to file
                return(json.dumps(schema_content))

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
            
            if self.parent_schema.textstring != '':
                # Read existing schema
                schema_content = json.loads(self.parent_schema.textstring)
                
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
                
                return(json.dumps(schema_content))
                # print(f"Added property '{self.name}' to {jsonschemafile}")
        
        def remove(self):
            """Remove a property from the schema"""
            if self.parent_schema.textstring != '':
                # Read existing schema
                schema_content = json.loads(self.parent_schema.textstring)
                
                # Remove property if it exists
                if "properties" in schema_content and self.name in schema_content["properties"]:
                    del schema_content["properties"][self.name]
                
                # Write back to file
                return(json.dumps(schema_content))
                
    class Required:
        def __init__(self, parent_schema, property_names):
            self.parent_schema = parent_schema
            self.property_names = property_names

        def add(self):
            """Add required properties to the schema"""

            if self.parent_schema.textstring != '':
                # Read existing schema
                schema_content = json.loads(self.parent_schema.textstring)
                
                # Ensure required array exists
                if "required" not in schema_content:
                    schema_content["required"] = []
                
                # Add required properties
                for prop in self.property_names:
                    if prop not in schema_content["required"]:
                        schema_content["required"].append(prop)
                
                # Write back to file
                return(json.dumps(schema_content))

        def remove(self):
            """Remove required properties from the schema"""

            if self.parent_schema.textstring != '':
                # Read existing schema
                schema_content = json.loads(self.parent_schema.textstring)
                
                # Remove required properties
                if "required" in schema_content:
                    schema_content["required"] = [
                        prop for prop in schema_content["required"]
                        if prop not in self.property_names
                    ]
                
                # Write back to file
                return(json.dumps(schema_content))

    class AdditionalProperties:
        def __init__(self, parent_schema, allowed):
            self.parent_schema = parent_schema
            self.allowed = allowed

        def modify(self):
            """Modify additionalProperties in the schema"""

            if self.parent_schema.textstring != '':
                # Read existing schema
                schema_content = json.loads(self.parent_schema.textstring)
                

                # Ensure additionalProperties exists
                if "additionalProperties" not in schema_content:
                    schema_content["additionalProperties"] = {}

                # Modify additionalProperties
                # schema_content["additionalProperties"] = self.allowed

                for prop,value in self.allowed.items():
                    if prop not in schema_content["additionalProperties"]:
                        schema_content["additionalProperties"][prop] = value

                # Write back to file
                return(json.dumps(schema_content))

        def remove(self):
            """Remove additionalProperties from the schema"""

            if self.parent_schema.textstring != '':
                # Read existing schema
                schema_content = json.loads(self.parent_schema.textstring)
                
                # Remove additionalProperties
                if "additionalProperties" in schema_content:
                    del schema_content["additionalProperties"]
                
                # Write back to file
                return(json.dumps(schema_content))

def test_schema():
    """Main function"""
    jsonschema = ''
    
    jsonschema = schema("test", jsonschema).create()
    print('test create object:\n\t' + jsonschema)
    
    jsonschemadescription = 'This is a jsonschema file defining the table for validating a test record'
    jsonschema = schema("test", jsonschema).description(jsonschemadescription).modify()
    print('test modify object description:\n\t' + jsonschema)

    jsonschema = schema("test", jsonschema).properties("testkey", "The unique identifier for the test", "number").add()
    print('test add properties:\n\t' + jsonschema)

    jsonschema = schema("test", jsonschema).properties("testname", "The name for test", "string").add()
    print('test add properties:\n\t' + jsonschema)

    jsonschema = schema("test", jsonschema).properties("testdesc", "The description for test", "string").add()
    print('test add properties:\n\t' + jsonschema)
    
    jsonschema = schema("test", jsonschema).properties("testdesc").remove()
    print('test remove properties:\n\t' + jsonschema)

    jsonschema = schema("test", jsonschema).required(["testname"]).add()
    print('test add required:\n\t' + jsonschema)

    jsonschema = schema("test", jsonschema).additionalproperties({"primarykeycolumn": "testkey"}).modify()
    print('test modify additionalproperties:\n\t' + jsonschema)

    jsonschema = schema("test", jsonschema).additionalproperties({"uniquecolumns": ["testname"]}).modify()
    print('test modify additionalproperties:\n\t' + jsonschema)

    return(jsonschema)

print('\nFull jsonschema text:\n' + json.dumps(json.loads(test_schema()), indent=4))