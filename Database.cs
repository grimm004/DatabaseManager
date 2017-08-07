using System;
using System.Collections.Generic;
using System.IO;

namespace DatabaseManager
{
    #region Datatype
    public enum Datatype
    {
        Number,
        Integer,
        VarChar,
    }
    #endregion

    #region Database
    public abstract class Database
    {
        public List<Table> tables;
        protected string name;
        protected string tableFileExtention;

        public abstract void CreateTable(string tableName, Fields fields, bool ifNotExists = true);
        public abstract Table GetTable(string tableName);
        public abstract void DeleteTable(string tableName);
        public int TableCount { get { return tables.Count; } }
        
        public abstract Record GetRecordByID(string tableName, int ID);
        public abstract Record GetRecord(string tableName, string conditionField, object conditionValue);
        public abstract Record[] GetRecords(string tableName, string conditionField, object conditionValue);

        public abstract Record AddRecord(string tableName, object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null);
        
        public abstract Record UpdateRecord(string tableName, Record record, object[] values);
        public abstract Record UpdateRecord(string tableName, Record record, string fieldString, object[] value);
        public abstract Record UpdateRecord(string tableName, int ID, object[] values);
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
    #endregion

    #region Table
    public abstract class Table
    {
        public string Name { get; protected set; }
        public string FileName { get; protected set; }
        public Fields Fields { get; protected set; }
        protected List<Record> RecordCache { get; set; }
        protected bool Edited { get; set; }

        public abstract int RecordCount { get; }
        public int FieldCount { get { return Fields.Count; } }

        public Table(string fileName, string name, Fields fields)
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

        public abstract Record GetRecordByID(int ID);
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
        public abstract Record UpdateRecord(int ID, object[] values);

        public bool RecordExists(string conditionField, object conditionValue)
        {
            foreach (Record record in RecordCache) if (record.GetValue(conditionField) == conditionValue) return true;
            return false;
        }

        public override string ToString()
        {
            string fieldList = "";
            foreach (string field in Fields.fieldNames) fieldList += string.Format("{0}, ", field);
            if (Fields.Count > 0) fieldList = fieldList.Remove(fieldList.Length - 2, 2);
            return string.Format("Table('{0}', {1} {2} ({3}), {4} {5})", Name, FieldCount, (FieldCount == 1) ? "field" : "fields", fieldList, RecordCount, (RecordCount == 1) ? "record" : "records");
        }

        public abstract void MarkForUpdate();

        public abstract void Save();
    }
    #endregion

    #region Fields
    public class Fields
    {
        public string[] fieldNames;
        public Datatype[] fieldTypes;
        public int[] fieldSizes;
        public int[] fieldOffsets;

        public int Size { get; protected set; }
        public int RecordSize { get; protected set; }

        public int Count { get { return fieldNames.Length; } }

        public int GetFieldID(string fieldName)
        {
            return Array.IndexOf(fieldNames, fieldName);
        }

        public Datatype GetFieldType(string fieldName)
        {
            return fieldTypes[Array.IndexOf(fieldNames, fieldName)];
        }

        public override string ToString()
        {
            string fieldData = "";
            for (int i = 0; i < Count; i++) fieldData += string.Format("{0} ({1}), ", fieldNames[i], fieldTypes[i]);
            if (Count > 0) fieldData = fieldData.Remove(fieldData.Length - 2, 2);
            return string.Format("Fields({0})", fieldData);
        }
    }
    #endregion

    #region Record
    public abstract class Record
    {
        public int ID;
        protected Fields fields;
        protected object[] values;
        private const int maxStringLength = 10;

        public abstract object GetValue(string field);

        public abstract void SetValue(string field, object value);

        public override string ToString()
        {
            string rowData = "";
            for (int i = 0; i < fields.Count; i++)
            {
                switch (fields.fieldTypes[i])
                {
                    case Datatype.Number:
                        rowData += string.Format("{0:0.0000}, ", (double)values[i]);
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
            }
            if (fields.Count > 0) rowData = rowData.Remove(rowData.Length - 2, 2);
            return string.Format("Record(ID {0}, Values ({1}))", ID, rowData);
        }
    }
    #endregion

    #region Exceptions
    class InvalidHeaderException : Exception
    {
        public InvalidHeaderException() : base("Invalid or no table header found.") { }
    }
    #endregion
}
