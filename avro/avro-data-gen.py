from fastavro import reader, writer, parse_schema
import json

inpFile = str(input("Enter the avro data file name:  "))
inpSchemaFile = str(input("Enter the avro schema file name:  "))
outFile = str(input("Enter the avro output file:   ")) or "outdata.avro"


with open(inpSchemaFile,'rb') as sc:
    schema = sc.read()

parsed = parse_schema(json.loads(schema))


with open(inpFile,'rb') as inp:
   records =  [r for r in reader(inp)]
   records.append(records[-1])
   flag=1
   while flag:
       field = str(input("Which field you want to edit:    "))
       if '.' in field:
           pass
       else:
           records[-1][field] = int(input("Enter the value for "+ field+":   "))
       flag = int(input("Press 1 to continue or 0 to halt: "))
   print(records[-1])





with open(outFile, 'wb') as out:
    writer(out, parsed, records)



