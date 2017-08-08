using System;
using System.Collections.Generic;
using System.IO;

namespace DatabaseManager
{
    public enum Datatype
    {
        Number,
        Integer,
        VarChar,
        Null,
    }
    
    public abstract class Database
    {
        public List<Table> tables;
        protected string name;
        protected string tableFileExtention;

        public abstract void CreateTable(string tableName, TableFields fields, bool ifNotExists = true);
        public abstract Table GetTable(string tableName);
        public abstract void DeleteTable(string tableName);
        public int TableCount { get { return tables.Count; } }
        
        public abstract Record GetRecordByID(string tableName, uint ID);
        public abstract Record GetRecord(string tableName, string conditionField, object conditionValue);
        public abstract Record[] GetRecords(string tableName, string conditionField, object conditionValue);

        public abstract Record AddRecord(string tableName, object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null);
        
        public abstract Record UpdateRecord(string tableName, Record record, object[] values);
        public abstract Record UpdateRecord(string tableName, Record record, string fieldString, object[] value);
        public abstract Record UpdateRecord(string tableName, uint ID, object[] values);
        public abstract Record[] UpdateRecords(string tableName, string fieldString, object[] values, string conditionField, object conditionValue);
        
        public override string ToString()
        {
            string tableList = "";
            foreach (Table table in tables) tableList += string.Format("'{0}', ", table.Name);
            if (TableCount > 0) tableList = tableList.Remove(tableList.Length - 2, 2);
            return string.Format("Database('{0}', {1} {2} ({3}))", name, TableCount, (TableCount == 1) ? "table" : "tables", tableList);
        }
        
        public void Output()
        {
            if (tables.Count > 0) Console.WriteLine("{0}:", ToString());
            else Console.WriteLine("{0}", ToString());
            foreach (Table table in tables)
            {
                if (table.RecordCount > 0) Console.WriteLine("  > {0}:", table);
                else Console.WriteLine("  > {0}", table);
                foreach (Record record in table.GetRecords()) Console.WriteLine("    - {0}", record);
            }
        }
        
        public void SaveChanges()
        {
            foreach (Table table in tables) table.Save();
        }
    }
    
    public abstract class Table
    {
        public string Name { get; protected set; }
        public string FileName { get; protected set; }
        public TableFields Fields { get; protected set; }
        protected List<Record> RecordCache { get; set; }
        protected bool Edited { get; set; }

        public abstract uint RecordCount { get; }
        public int FieldCount { get { return Fields.Count; } }

        public Table(string fileName, string name, TableFields fields)
        {
            this.Fields = fields;
            this.Name = name;
            this.FileName = fileName;
            RecordCache = new List<Record>();
            Edited = true;
        }

        public Table(string fileName)
        {
            this.FileName = fileName;
            this.Name = Path.GetFileNameWithoutExtension(fileName);
            RecordCache = new List<Record>();
            LoadTable();
            Edited = false;
        }

        public abstract void LoadTable();

        public abstract Record GetRecordByID(uint ID);
        public abstract Record[] GetRecords();
        public abstract Record[] GetRecords(string conditionField, object conditionValue);
        public abstract Record GetRecord(string conditionField, object conditionValue);
        public abstract void SearchRecords(Action<Record> callback);

        public Record AddRecord(object[] values)
        {
            return AddRecord(values, false, null, null);
        }
        public abstract Record AddRecord(object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null);

        public abstract Record UpdateRecord(Record record, object[] values);
        public abstract Record UpdateRecord(Record record, string fieldString, object value);
        public abstract Record[] UpdateRecords(string fieldString, object[] values, string conditionField, object conditionValue);
        public abstract Record UpdateRecord(uint ID, object[] values);

        public bool RecordExists(string conditionField, object conditionValue)
        {
            foreach (Record record in RecordCache) if (record.GetValue(conditionField) == conditionValue) return true;
            return false;
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

        public int Count { get { return Fields.Length; } }

        public int GetFieldID(string fieldName)
        {
            for (int i = 0; i < Count; i++) if (Fields[i].Name == fieldName) return i;
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
    }

    public abstract class Field
    {
        public string Name { get; set; }
        public Datatype DataType { get; set; }

        public Field() { Name = ""; DataType = Datatype.Null; }
        public Field(string name, Datatype dataType) { Name = name; DataType = dataType; }

        public override string ToString()
        {
            return string.Format("Field(Name: '0', DataType: '{1}')", Name, DataType);
        }
    }

    public abstract class Record
    {
        public uint ID;
        protected TableFields fields;
        protected object[] values;
        private const int maxStringLength = 10;

        public abstract object GetValue(string field);

        public abstract void SetValue(string field, object value);

        public override string ToString()
        {
            string rowData = "";
            for (int i = 0; i < fields.Count; i++)
                switch (fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        rowData += string.Format("{0:0.0000}, ", values[i]);
                        break;
                    case Datatype.Integer:
                        rowData += string.Format("{0}, ", (int)values[i]);
                        break;
                    case Datatype.VarChar:
                        string outputString = (string)values[i];
                        if (outputString.Length > maxStringLength) outputString = outputString.Substring(0, maxStringLength);
                        rowData += string.Format("'{0}', ", outputString);
                        break;
                }
            if (fields.Count > 0) rowData = rowData.Remove(rowData.Length - 2, 2);
            return string.Format("Record(ID {0}, Values ({1}))", ID, rowData);
        }
    }
    
    class InvalidHeaderException : Exception
    {
        public InvalidHeaderException() : base("Invalid or no table header found.") { }
    }
}
