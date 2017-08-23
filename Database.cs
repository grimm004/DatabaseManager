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

    public abstract class Database
    {
        public List<Table> Tables { get; protected set; }
        public string Name { get; protected set; }
        protected string TableFileExtention { get; set; }

        public abstract void CreateTable(string tableName, TableFields fields, bool ifNotExists = true);
        public bool AddTable(Table newTable)
        {
            foreach (Table table in Tables) if (table.Name == newTable.Name) return false;
            Tables.Add(newTable);
            return true;
        }
        public Table GetTable(string tableName)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table;
            return null;
        }
        public void DeleteTable(string tableName)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) Tables.Remove(table);
        }
        public int TableCount { get { return Tables.Count; } }

        public void UpdateField(string tableName, string fieldName, string newFieldName)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) table.UpdateField(fieldName, newFieldName);
        }
        
        public Record GetRecordByID(string tableName, uint ID)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.GetRecordByID(ID);
            return null;
        }
        public Record GetRecord(string tableName, string conditionField, object conditionValue)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.GetRecord(conditionField, conditionValue);
            return null;
        }
        public Record[] GetRecords(string tableName, string conditionField, object conditionValue)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.GetRecords(conditionField, conditionValue);
            return null;
        }

        public Record AddRecord(string tableName, object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.AddRecord(values, ifNotExists, conditionField, conditionValue);
            return null;
        }
        public void UpdateRecord(string tableName, Record record, object[] values)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) table.UpdateRecord(record, values);
        }
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
        
        public void SaveChanges()
        {
            foreach (Table table in Tables) table.Save();
        }
    }

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

    public abstract class Table
    {
        public string Name { get; protected set; }
        public string FileName { get; protected set; }
        public TableFields Fields { get; protected set; }
        protected ChangeCache Changes { get; set; }
        protected bool Edited { get; set; }

        public abstract uint RecordCount { get; }
        public int FieldCount { get { return Fields.Count; } }

        public Table(string fileName, string name, TableFields fields)
        {
            this.Fields = fields;
            this.Name = name;
            this.FileName = fileName;
            Changes = new ChangeCache();
            Edited = true;
        }
        public Table(string fileName)
        {
            this.FileName = fileName;
            this.Name = Path.GetFileNameWithoutExtension(fileName);
            Changes = new ChangeCache();
            LoadTable();
            Edited = false;
        }
        public abstract void LoadTable();

        public abstract Record GetRecordByID(uint ID);
        public abstract Record[] GetRecords();
        public abstract Record[] GetRecords(string conditionField, object conditionValue);
        public abstract Record GetRecord(string conditionField, object conditionValue);
        public abstract void SearchRecords(Action<Record> callback);

        public Record AddRecord(Record record)
        {
            MarkForUpdate();
            Changes.AddedRecords.Add(record);
            return record;
        }
        public Record AddRecord(object[] values)
        {
            return AddRecord(values, false, null, null);
        }
        public abstract Record AddRecord(object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null);
        
        public void UpdateField(string fieldName, string newFieldName)
        {
            MarkForUpdate();
            Fields.MarkForUpdate();
            foreach (Field field in Fields.Fields) if (field.Name.ToLower() == fieldName.ToLower()) field.Update(newFieldName);
        }

        public abstract void UpdateRecord(Record record, object[] values);
        public abstract void DeleteRecord(Record record);

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

        public abstract void MarkForUpdate();
        public abstract void Save();
    }

    public class TableFields
    {
        public Field[] Fields { get; set; }
        public bool Edited { get; protected set; }
        public int Count { get { return Fields.Length; } }
        public int GetFieldID(string fieldName)
        {
            for (int i = 0; i < Count; i++) if (Fields[i].Name.ToLower() == fieldName.ToLower()) return i;
            return -1;
        }
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
        public void MarkForUpdate()
        {
            Edited = true;
        }
    }

    public abstract class Field
    {
        public string Name { get; protected set; }
        public Datatype DataType { get; protected set; }

        public Field() { Name = ""; DataType = Datatype.Null; }
        public Field(string name, Datatype dataType) { Name = name; DataType = dataType; }

        public override string ToString()
        {
            return string.Format("Field(Name: '0', DataType: '{1}')", Name, DataType);
        }
        public void Update(string name)
        {
            Name = name;
        }
    }

    public abstract class Record
    {
        public uint ID;
        protected TableFields Fields { get; set; }
        protected object[] values;
        public static int maxStringOutputLength = 10;

        public object GetValue(string field)
        {
            for (int i = 0; i < Fields.Count; i++) if (Fields.Fields[i].Name == field) return values[i];
            return null;
        }
        public T GetValue<T>(string field)
        {
            for (int i = 0; i < Fields.Count; i++) if (Fields.Fields[i].Name == field) return (T)values[i];
            throw new FieldNotFoundException(field);
        }
        public object[] GetValues()
        {
            return values;
        }
        public void SetValue(string field, object value)
        {
            if (value != null)
            {
                int fieldIndex = -1;
                for (int i = 0; i < Fields.Count; i++) if (Fields.Fields[i].Name == field) fieldIndex = i;
                if (fieldIndex == -1) throw new FieldNotFoundException(field);
                values[fieldIndex] = value;
                switch (Fields.Fields[fieldIndex].DataType)
                {
                    case Datatype.Number:
                        values[fieldIndex] = Convert.ToDouble(value);
                        break;
                    case Datatype.VarChar:
                        values[fieldIndex] = (string)value;
                        break;
                    case Datatype.Integer:
                        values[fieldIndex] = (int)value;
                        break;
                    case Datatype.DateTime:
                        values[fieldIndex] = value;
                        break;
                }
            }
        }

        public override string ToString()
        {
            string rowData = "";
            for (int i = 0; i < Fields.Count; i++)
                switch (Fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        rowData += string.Format("{0:0.0000}, ", values[i]);
                        break;
                    case Datatype.Integer:
                        rowData += string.Format("{0}, ", (int)values[i]);
                        break;
                    case Datatype.VarChar:
                        string outputString = (string)values[i];
                        if (outputString.Length > maxStringOutputLength)
                            outputString = outputString.Substring(0, maxStringOutputLength);
                        rowData += string.Format("'{0}', ", outputString);
                        break;
                    case Datatype.DateTime:
                        rowData += string.Format("{0}, ", values[i] != null ? ((DateTime)values[i]).ToString() : "null");
                        break;
                }
            if (Fields.Count > 0) rowData = rowData.Remove(rowData.Length - 2, 2);
            return string.Format("Record(ID {0}, Values ({1}))", ID, rowData);
        }
        public T ToObject<T>() where T : class, new()
        {
            T recordObject = new T();
            Type recordObjectType = recordObject.GetType();
            for (int i = 0; i < Fields.Count; i++)
                recordObjectType.GetProperty(Fields.Fields[i].Name)?.SetValue(recordObject, values[i], null);
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
