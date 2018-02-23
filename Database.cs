using System;
using System.Collections.Generic;
using System.IO;

namespace DatabaseManagerLibrary
{
    public enum Datatype
    {
        Number,
        Integer,
        VarChar,
        DateTime,
        Null,
    }

    /// <summary>
    /// An abstract database model
    /// </summary>
    public abstract class Database
    {
        public List<Table> Tables { get; protected set; }
        protected List<Table> DeletedTables { get; set; }
        public string Name { get; protected set; }
        protected string TableFileExtention { get; set; }

        /// <summary>
        /// Instanciate the database
        /// </summary>
        public Database()
        {
            Tables = new List<Table>();
            DeletedTables = new List<Table>();
        }

        /// <summary>
        /// Create a new table
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="fields">The table's fields</param>
        /// <param name="ifNotExists">Create if it does not exist</param>
        /// <returns>the created table</returns>
        public abstract Table CreateTable(string tableName, TableFields fields, bool ifNotExists = true);

        /// <summary>
        /// Add a table
        /// </summary>
        /// <param name="newTable">The table to add</param>
        /// <returns>true if the table is added</returns>
        public bool AddTable(Table newTable)
        {
            foreach (Table table in Tables) if (table.Name == newTable.Name) return false;
            Tables.Add(newTable);
            return true;
        }

        /// <summary>
        /// Check if a table exists
        /// </summary>
        /// <param name="tableName">The table name to check</param>
        /// <returns>true if the table exists</returns>
        public bool TableExists(string tableName)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return true;
            return false;
        }

        /// <summary>
        /// Get a table
        /// </summary>
        /// <param name="tableName">The table to get</param>
        /// <returns>the desired table if found</returns>
        public Table GetTable(string tableName)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table;
            return null;
        }

        /// <summary>
        /// Delete a table
        /// </summary>
        /// <param name="tableName">The name of the table to delete</param>
        public void DeleteTable(string tableName)
        {
            for (int i = 0; i < TableCount; i++) if (Tables[i].Name.ToLower() == tableName.ToLower()) DeletedTables.Add(Tables[i]);
        }
        
        public int TableCount { get { return Tables.Count; } }

        /// <summary>
        /// Change the name of a field
        /// </summary>
        /// <param name="tableName">The name of the field's table</param>
        /// <param name="fieldName">The name of the field</param>
        /// <param name="newFieldName">The new name of the table</param>
        public void UpdateField(string tableName, string fieldName, string newFieldName)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) table.UpdateField(fieldName, newFieldName);
        }
        
        /// <summary>
        /// Get a record by its ID
        /// </summary>
        /// <param name="tableName">The table record's table name</param>
        /// <param name="ID">The record's ID</param>
        /// <returns>the desired record if found</returns>
        public Record GetRecordByID(string tableName, uint ID)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.GetRecordByID(ID);
            return null;
        }

        /// <summary>
        /// Get a record based on a condition
        /// </summary>
        /// <param name="tableName">The record's table name</param>
        /// <param name="conditionField">The field of the condition</param>
        /// <param name="conditionValue">The value of the condition</param>
        /// <returns>the first matching record</returns>
        public Record GetRecord(string tableName, string conditionField, object conditionValue)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.GetRecord(conditionField, conditionValue);
            return null;
        }

        /// <summary>
        /// Get all records matching a condition
        /// </summary>
        /// <param name="tableName">The records' table name</param>
        /// <param name="conditionField">The field of the condition</param>
        /// <param name="conditionValue">The value of the condition</param>
        /// <returns>all matching records</returns>
        public Record[] GetRecords(string tableName, string conditionField, object conditionValue)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.GetRecords(conditionField, conditionValue);
            return new Record[0];
        }

        /// <summary>
        /// Get all records
        /// </summary>
        /// <param name="tableName">The table to get the records from</param>
        /// <returns>all the records in the table</returns>
        public Record[] GetRecords(string tableName)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.GetRecords();
            return new Record[0];
        }

        /// <summary>
        /// Get all records as an object
        /// </summary>
        /// <typeparam name="T">The type to try convert records to</typeparam>
        /// <param name="tableName">The name of the table to fetch records from</param>
        /// <returns>all the records as the desired type</returns>
        public T[] GetRecords<T>(string tableName) where T : class, new()
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.GetRecords<T>();
            return new T[0];
        }

        /// <summary>
        /// Add a record
        /// </summary>
        /// <param name="tableName">The name of the table to add the record to</param>
        /// <param name="values">The values in the record</param>
        /// <param name="ifNotExists">Only add if it does not exist based on conditions</param>
        /// <param name="conditionField">The field to check for an exising record</param>
        /// <param name="conditionValue">The condition to check against</param>
        /// <returns>the newly created record</returns>
        public Record AddRecord(string tableName, object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.AddRecord(values, ifNotExists, conditionField, conditionValue);
            return null;
        }

        /// <summary>
        /// Update the values in a record
        /// </summary>
        /// <param name="tableName">The name of the table containing the record to be updated</param>
        /// <param name="record">The record to update</param>
        /// <param name="values">The values to change to, null values are not changed</param>
        public void UpdateRecord(string tableName, Record record, object[] values)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) table.UpdateRecord(record, values);
        }
        
        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="tableName">The table containing the record to be deleted</param>
        /// <param name="record">The record to delete</param>
        public void DeleteRecord(string tableName, Record record)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) table.DeleteRecord(record);
        }

        public override string ToString()
        {
            string tableList = "";
            foreach (Table table in Tables) tableList += string.Format("'{0}', ", table.Name);
            if (TableCount > 0) tableList = tableList.Remove(tableList.Length - 2, 2);
            return string.Format("Database('{0}', {1} {2} ({3}))", Name, TableCount, (TableCount == 1) ? "table" : "tables", tableList);
        }
        
        public void Output()
        {
            if (Tables.Count > 0) Console.WriteLine("{0}:", ToString());
            else Console.WriteLine("{0}", ToString());
            foreach (Table table in Tables)
            {
                if (table.RecordCount > 0) Console.WriteLine("  > {0}:", table);
                else Console.WriteLine("  > {0}", table);
                foreach (Record record in table.GetRecords()) Console.WriteLine("    - {0}", record);
            }
        }
        
        /// <summary>
        /// Save changes to the database
        /// </summary>
        public void SaveChanges()
        {
            foreach (Table table in Tables) table.Save();
            foreach (Table deletedTable in DeletedTables) { File.Delete(deletedTable.FileName); Tables.Remove(deletedTable); }
            DeletedTables = new List<Table>();
        }
    }

    /// <summary>
    /// A storage of changes made to a database
    /// </summary>
    public class ChangeCache
    {
        public List<Record> AddedRecords { get; protected set; }
        public List<Record> ChangedRecords { get; protected set; }
        public List<Record> DeletedRecords { get; protected set; }

        public ChangeCache()
        {
            AddedRecords = new List<Record>();
            ChangedRecords = new List<Record>();
            DeletedRecords = new List<Record>();
        }
    }

    /// <summary>
    /// An abstract database table model
    /// </summary>
    public abstract class Table
    {
        public string Name { get; protected set; }
        public string FileName { get; protected set; }
        public TableFields Fields { get; protected set; }
        protected ChangeCache Changes { get; set; }
        protected bool Edited { get; set; }

        public abstract uint RecordCount { get; }
        public int FieldCount { get { return Fields.Count; } }

        /// <summary>
        /// Initialise the table
        /// </summary>
        /// <param name="fileName">The filename of the table</param>
        /// <param name="name">The name of the table</param>
        /// <param name="fields">The fields in the database</param>
        public Table(string fileName, string name, TableFields fields)
        {
            this.Fields = fields;
            this.Name = name;
            this.FileName = fileName;
            Changes = new ChangeCache();
            Edited = true;
        }

        /// <summary>
        /// The filename of the database
        /// </summary>
        /// <param name="fileName"></param>
        public Table(string fileName)
        {
            this.FileName = fileName;
            this.Name = Path.GetFileNameWithoutExtension(fileName);
            Changes = new ChangeCache();
            LoadTable();
            Edited = false;
        }

        /// <summary>
        /// Load the table
        /// </summary>
        public abstract void LoadTable();

        /// <summary>
        /// Get a record by its ID
        /// </summary>
        /// <param name="ID">The ID of the record to fetch</param>
        /// <returns>the record matching the desired ID</returns>
        public abstract Record GetRecordByID(uint ID);

        /// <summary>
        /// Get all records from the table
        /// </summary>
        /// <returns></returns>
        public abstract Record[] GetRecords();

        /// <summary>
        /// Get records matching a condition
        /// </summary>
        /// <param name="conditionField">Conditonal field to check</param>
        /// <param name="conditionValue">The value to look for</param>
        /// <returns>all records matching the condition value</returns>
        public abstract Record[] GetRecords(string conditionField, object conditionValue);

        /// <summary>
        /// Get the first record matching a condition
        /// </summary>
        /// <param name="conditionField">Conditional field to check</param>
        /// <param name="conditionValue">The value to look for</param>
        /// <returns>the first record matching the condition</returns>
        public abstract Record GetRecord(string conditionField, object conditionValue);

        /// <summary>
        /// Search through all records
        /// </summary>
        /// <param name="callback">The callback for when each record is ready to be searched</param>
        public abstract void SearchRecords(Action<Record> callback);

        /// <summary>
        /// Get all records as an object
        /// </summary>
        /// <typeparam name="T">The type to try convert records to</typeparam>
        /// <returns>the record as the desired type</returns>
        public T[] GetRecords<T>() where T : class, new()
        {
            Record[] records = GetRecords();
            T[] objects = new T[records.Length];
            for (int i = 0; i < records.Length; i++) objects[i] = records[i].ToObject<T>();
            return objects;
        }

        /// <summary>
        /// Add a new record
        /// </summary>
        /// <param name="record">The record to add</param>
        /// <returns>the newly added record</returns>
        public Record AddRecord(Record record)
        {
            MarkForUpdate();
            Changes.AddedRecords.Add(record);
            return record;
        }

        /// <summary>
        /// Add a new record
        /// </summary>
        /// <param name="record">The record's values</param>
        /// <returns>the newly added record</returns>
        public Record AddRecord(object[] values)
        {
            return AddRecord(values, false, null, null);
        }

        /// <summary>
        /// Add a new record
        /// </summary>
        /// <param name="values">The record's values</param>
        /// <param name="ifNotExists">Add the record if it does not exist</param>
        /// <param name="conditionField">The field to check if the record exists</param>
        /// <param name="conditionValue">The value to check if the record exists</param>
        /// <returns>the newly added record</returns>
        public abstract Record AddRecord(object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null);
        
        /// <summary>
        /// Update a field's name
        /// </summary>
        /// <param name="fieldName">The name of the field to change</param>
        /// <param name="newFieldName">The new name of the field</param>
        public void UpdateField(string fieldName, string newFieldName)
        {
            MarkForUpdate();
            Fields.MarkForUpdate();
            foreach (Field field in Fields.Fields) if (field.Name.ToLower() == fieldName.ToLower()) field.Update(newFieldName);
        }

        /// <summary>
        /// Update a record's values
        /// </summary>
        /// <param name="record">The record to update</param>
        /// <param name="values">The values to change to, null vlaues dont change</param>
        public abstract void UpdateRecord(Record record, object[] values);

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        public abstract void DeleteRecord(Record record);

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="id">The ID of the record to delete</param>
        public abstract void DeleteRecord(uint id);

        /// <summary>
        /// Check if a record exists
        /// </summary>
        /// <param name="conditionField">The field to check</param>
        /// <param name="conditionValue">The value to check for</param>
        /// <returns></returns>
        public bool RecordExists(string conditionField, object conditionValue)
        {
            throw new NotImplementedException();
        }

        public override string ToString()
        {
            string fieldList = "";
            foreach (Field field in Fields.Fields) fieldList += string.Format("{0}, ", field.Name);
            if (Fields.Count > 0) fieldList = fieldList.Remove(fieldList.Length - 2, 2);
            return string.Format("Table('{0}', {1} {2} ({3}), {4} {5})", Name, FieldCount, (FieldCount == 1) ? "field" : "fields", fieldList, RecordCount, (RecordCount == 1) ? "record" : "records");
        }

        /// <summary>
        /// Mark the table as needing an update
        /// </summary>
        public abstract void MarkForUpdate();

        /// <summary>
        /// Save the table
        /// </summary>
        public abstract void Save();
    }

    /// <summary>
    /// An abstract database table fields model
    /// </summary>
    public abstract class TableFields
    {
        public Field[] Fields { get; set; }
        public bool Edited { get; protected set; }
        public int Count { get { return Fields.Length; } }

        /// <summary>
        /// Get the ID of a field
        /// </summary>
        /// <param name="fieldName">The name of the field</param>
        /// <returns>the ID of the corresponding field</returns>
        public int GetFieldID(string fieldName)
        {
            for (int i = 0; i < Count; i++) if (Fields[i].Name.ToLower() == fieldName.ToLower()) return i;
            return -1;
        }

        /// <summary>
        /// Get the datatype of a field
        /// </summary>
        /// <param name="fieldName">The name of the field</param>
        /// <returns>the datatype of the field</returns>
        public Datatype GetFieldType(string fieldName)
        {
            foreach (Field field in Fields) if (field.Name == fieldName) return field.DataType;
            return Datatype.Null;
        }
        public override string ToString()
        {
            string fieldData = "";
            for (int i = 0; i < Count; i++) fieldData += string.Format("{0} ({1}), ", Fields[i].Name, Fields[i].DataType);
            if (Count > 0) fieldData = fieldData.Remove(fieldData.Length - 2, 2);
            return string.Format("Fields({0})", fieldData);
        }
        /// <summary>
        /// Mark the field as needing an update
        /// </summary>
        public void MarkForUpdate()
        {
            Edited = true;
        }
    }

    /// <summary>
    /// An abstract database field model
    /// </summary>
    public abstract class Field
    {
        public string Name { get; protected set; }
        public Datatype DataType { get; protected set; }

        /// <summary>
        /// Initialise the field
        /// </summary>
        public Field() { Name = ""; DataType = Datatype.Null; }
        /// <summary>
        /// Initialise the field
        /// </summary>
        /// <param name="name">The name of the field</param>
        /// <param name="dataType">The datatype of the field</param>
        public Field(string name, Datatype dataType) { Name = name; DataType = dataType; }

        public override string ToString()
        {
            return string.Format("Field(Name: '0', DataType: '{1}')", Name, DataType);
        }
        /// <summary>
        /// Update the name of the field
        /// </summary>
        /// <param name="name">The new name of the field</param>
        public void Update(string name)
        {
            Name = name;
        }
    }

    /// <summary>
    /// An abstract database record model
    /// </summary>
    public abstract class Record
    {
        public uint ID { get; set; }
        protected TableFields Fields { get; set; }
        protected object[] Values { get; set; }

        /// <summary>
        /// Get a value
        /// </summary>
        /// <param name="field">The field of the value to fetch</param>
        /// <returns></returns>
        public object GetValue(string field)
        {
            for (int i = 0; i < Fields.Count; i++) if (Fields.Fields[i].Name == field) return Values[i];
            return null;
        }

        /// <summary>
        /// Get a casted value
        /// </summary>
        /// <typeparam name="T">The datatype to cast to</typeparam>
        /// <param name="field">The field of the value</param>
        /// <returns>the casted value</returns>
        public T GetValue<T>(string field)
        {
            for (int i = 0; i < Fields.Count; i++) if (Fields.Fields[i].Name == field) return (T)Values[i];
            throw new FieldNotFoundException(field);
        }

        /// <summary>
        /// Get all the values
        /// </summary>
        /// <returns>an array of objects containing the values</returns>
        public object[] GetValues()
        {
            return Values;
        }

        /// <summary>
        /// Set a value in the record
        /// </summary>
        /// <param name="field">The field of the value to set</param>
        /// <param name="value">The value to set</param>
        public void SetValue(string field, object value)
        {
            if (value != null)
            {
                int fieldIndex = -1;
                for (int i = 0; i < Fields.Count; i++) if (Fields.Fields[i].Name == field) fieldIndex = i;
                if (fieldIndex == -1) throw new FieldNotFoundException(field);
                Values[fieldIndex] = value;
                switch (Fields.Fields[fieldIndex].DataType)
                {
                    case Datatype.Number:
                        Values[fieldIndex] = Convert.ToDouble(value);
                        break;
                    case Datatype.VarChar:
                        Values[fieldIndex] = (string)value;
                        break;
                    case Datatype.Integer:
                        Values[fieldIndex] = (int)value;
                        break;
                    case Datatype.DateTime:
                        Values[fieldIndex] = value;
                        break;
                }
            }
        }
        
        public const int maxStringOutputLength = 10;
        
        public override string ToString()
        {
            string rowData = "";
            for (int i = 0; i < Fields.Count; i++)
                switch (Fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        rowData += string.Format("{0:0.0000}, ", Values[i]);
                        break;
                    case Datatype.Integer:
                        rowData += string.Format("{0}, ", (int)Values[i]);
                        break;
                    case Datatype.VarChar:
                        string outputString = (string)Values[i];
                        if (outputString.Length > maxStringOutputLength)
                            outputString = outputString.Substring(0, maxStringOutputLength);
                        rowData += string.Format("'{0}', ", outputString);
                        break;
                    case Datatype.DateTime:
                        rowData += string.Format("{0}, ", Values[i] != null ? ((DateTime)Values[i]).ToString() : "null");
                        break;
                }
            if (Fields.Count > 0) rowData = rowData.Remove(rowData.Length - 2, 2);
            return string.Format("Record(ID {0}, Values ({1}))", ID, rowData);
        }

        /// <summary>
        /// Convert the record to an object
        /// </summary>
        /// <typeparam name="T">The datatype to convert to</typeparam>
        /// <returns>the converted object</returns>
        public T ToObject<T>() where T : class, new()
        {
            T recordObject = new T();
            Type recordObjectType = recordObject.GetType();
            for (int i = 0; i < Fields.Count; i++)
                recordObjectType.GetProperty(Fields.Fields[i].Name)?.SetValue(recordObject, Values[i], null);
            return recordObject;
        }
    }

    class InvalidHeaderException : Exception
    {
        public InvalidHeaderException() : base("Invalid or no table header found.") { }
    }

    class FieldNotFoundException : Exception
    {
        public FieldNotFoundException() : base("Could not find field.") { }
        public FieldNotFoundException(string fieldName) : base(string.Format("Could not find field '{0}'.", fieldName)) { }
    }
}
