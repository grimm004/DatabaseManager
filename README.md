# DatabaseManager
A basic object-oriented abstraction for CSV and binary database files.

This is part of a school project. It still has multiple bugs and needs further testing.

The database structure works as follows:
  - Database Name
    - Table Name(.table/.csv)
    - Other Table Name(.table/.csv)
  - Other Database Name
    - Table Name(.table/.csv)
    - Etc...

For CSV files the first line must reference the name of each field and the datatype of the field.
Example:
First Line: username:string,xPosition:number,yPosition:number,health:integer
Second Line: "grimm004",2.0,4.0,100

The binary database is stored in its own format.
To make searching faster strings are of fixed length (like a varchar).
At the moment the string length is 1024 bytes for all fields prefixed by a 2 byte short. In the future it would be beneficial to allow this to be set per-table or even per-field.
Integers are stored as a standard 4 byte int32. Decimal numbers are stored as 4 byte floating point.
