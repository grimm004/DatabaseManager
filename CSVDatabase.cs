using System;
using System.Collections.Generic;
using System.IO;

namespace DatabaseManager
{
    #region Database
    public class CSVDatabase : Database
    {
        public CSVDatabase(string name, bool createIfNotExists = true, string tableExtention = ".table")
        {
            if (!Directory.Exists(name) && createIfNotExists) Directory.CreateDirectory(name);
            string[] tableFiles = Directory.GetFiles(name, string.Format("*{0}", tableExtention));
            tables = new List<Table>();
            foreach (string tableFile in tableFiles) tables.Add(new CSVTable(tableFile));
            this.name = name;
        }
        
        public override Table GetTable(string tableName)
        {
            foreach (Table table in tables) if (table.name == tableName) return table;
            return null;
        }

        public void CreateTable(string tableName, CSVFields fields, bool ifNotExists = true)
        {
            string fileName = string.Format("{0}\\{1}.table", name, tableName);
            if ((File.Exists(fileName) && !ifNotExists) || !File.Exists(fileName)) tables.Add(new CSVTable(fileName, tableName, fields));
        }

        public override void DeleteTable(string tableName)
        {
            foreach (Table table in tables) if (table.name == tableName) tables.Remove(table);
        }
        
        public override Record AddRecord(string tableName, object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            foreach (Table table in tables) if (table.name == tableName) return table.AddRecord(values, ifNotExists, conditionField, conditionValue);
            return null;
        }

        public override Record GetRecordByID(string tableName, int ID)
        {
            foreach (Table table in tables) if (table.name.ToLower() == tableName.ToLower()) return table.GetRecordByID(ID);
            return null;
        }

        public override Record GetRecord(string tableName, string conditionField, object conditionValue)
        {
            foreach (Table table in tables) if (table.name == tableName) return table.GetRecords(conditionField, conditionValue)[0];
            return null;
        }

        public override Record[] GetRecords(string tableName, string conditionField, object conditionValue)
        {
            foreach (Table table in tables) if (table.name == tableName) return table.GetRecords(conditionField, conditionValue);
            return null;
        }

        public override Record UpdateRecord(string tableName, Record record, object[] values)
        {
            foreach (Table table in tables) if (table.name == tableName) return table.UpdateRecord(record, values);
            return null;
        }

        public override Record UpdateRecord(string tableName, Record record, string fieldString, object[] value)
        {
            foreach (Table table in tables) if (table.name == tableName) return table.UpdateRecord(record, fieldString, value);
            return null;
        }

        public override Record[] UpdateRecords(string tableName, string fieldString, object[] values, string conditionField, object conditionValue)
        {
            foreach (Table table in tables) if (table.name == tableName) return table.UpdateRecords(fieldString, values, conditionField, conditionValue);
            return null;
        }

        public override Record UpdateRecord(string tableName, int ID, object[] values)
        {
            foreach (Table table in tables) if (table.name == tableName) return table.UpdateRecord(ID, values);
            return null;
        }

        public override void DeleteRecord(string tableName, Record record)
        {
            foreach (Table table in tables) if (table.name == tableName) table.DeleteRecord(record);
        }

        public override void DeleteRecord(string tableName, int ID)
        {
            foreach (Table table in tables) if (table.name == tableName) table.DeleteRecord(ID);
        }

        public override string ToString()
        {
            string tableList = "";
            foreach (Table table in tables) tableList += string.Format("'{0}', ", table.name);
            if (TableCount > 0) tableList = tableList.Remove(tableList.Length - 2, 2);
            return string.Format("Database('{0}', {1} {2} ({3}))", name, TableCount, (TableCount == 1) ? "table" : "tables", tableList);
        }
    }
    #endregion

    #region Table
    public class CSVTable : Table
    {
        public CSVTable(string fileName, string name, CSVFields fields) : base(fileName, name, fields)
        { }

        public CSVTable(string fileName) : base(fileName)
        { }
        
        public override int RecordCount { get { return records.Count; } }

        public override void LoadTable()
        {
            StreamReader sr = new StreamReader(fileName);
            string fieldData = sr.ReadLine();
            if (fieldData != null && fieldData.Contains(":"))
            {
                fields = new CSVFields(fieldData);
                records = new List<Record>();
                string currentLine;
                int currentRecordId = 0;
                while ((currentLine = sr.ReadLine()) != "" && currentLine != null)
                    records.Add(new CSVRecord(currentLine, currentRecordId++, fields));
            }
            else
            {
                fields = new CSVFields();
                records = new List<Record>();
            }
            sr.Close();
        }

        public override Record GetRecordByID(int ID)
        {
            foreach (Record record in records) if (record.ID == ID) return record;
            return null;
        }

        public override Record GetRecord(string conditionField, object conditionValue)
        {
            return GetRecords(conditionField, conditionValue)[0];
        }

        public override Record[] GetRecords(string conditionField, object conditionValue)
        {
            List<Record> resultRecords = new List<Record>();
            foreach (Record record in records)
            {
                Datatype type = fields.GetFieldType(conditionField);
                switch (type)
                {
                    case Datatype.Number:
                        if ((float)record.GetValue(conditionField) == (float)conditionValue) resultRecords.Add(record);
                        break;
                    case Datatype.Integer:
                        if ((int)record.GetValue(conditionField) == (int)conditionValue) resultRecords.Add(record);
                        break;
                    case Datatype.VarChar:
                        if ((string)record.GetValue(conditionField) == (string)conditionValue) resultRecords.Add(record);
                        break;
                }
            }
            return resultRecords.ToArray();
        }

        public Record AddRecord(string[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            return AddRecord(string.Join(",", values), ifNotExists, conditionField, conditionValue);
        }

        public Record AddRecord(string valueString, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            edited = true;
            if ((!ifNotExists || conditionField == null || conditionValue == null)
                ||
                (ifNotExists && conditionField != null && conditionValue != null
                 && !RecordExists(conditionField, conditionValue)))
            {
                Record newRecord = new CSVRecord(valueString, GetCurrnetId(), fields);
                records.Add(newRecord);
                return newRecord;
            }
            return null;
        }

        public override Record AddRecord(object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            edited = true;
            if ((!ifNotExists || conditionField == null || conditionValue == null)
                ||
                (ifNotExists && conditionField != null && conditionValue != null
                 && !RecordExists(conditionField, conditionValue)))
            {
                Record newRecord = new CSVRecord(values, GetCurrnetId(), fields);
                records.Add(newRecord);
                return newRecord;
            }
            return null;
        }

        public override Record UpdateRecord(Record record, object[] values)
        {
            edited = true;
            for (int i = 0; i < FieldCount; i++) record.SetValue(fields.fieldNames[i], values[i]);
            return record;
        }

        public override Record UpdateRecord(Record record, string fieldString, object value)
        {
            edited = true;
            record.SetValue(fieldString, value);
            return record;
        }

        public override Record[] UpdateRecords(string fieldString, object[] values, string conditionField, object conditionValue)
        {
            Record[] records = GetRecords(conditionField, conditionValue);
            foreach (Record record in records) UpdateRecord(record, values);
            return records;
        }

        public override Record UpdateRecord(int ID, object[] values)
        {
            return UpdateRecord(GetRecordByID(ID), values);
        }

        public override void DeleteRecord(Record record)
        {
            edited = true;
            records.Remove(record);
        }

        public override void DeleteRecord(int ID)
        {
            edited = true;
            DeleteRecord(GetRecordByID(ID));
        }

        public int GetCurrnetId()
        {
            return RecordCount;
        }

        public override void MarkForUpdate()
        {
            edited = true;
        }

        public override void Save()
        {
            if (edited)
            {
                edited = false;
                StreamWriter sr = new StreamWriter(fileName);
                sr.WriteLine(((CSVFields)fields).GetFileString());
                foreach (CSVRecord record in records) sr.WriteLine(record.GetFileString());
                sr.Close();
            }
        }
    }
    #endregion

    #region Fields
    public class CSVFields : Fields
    {
        public CSVFields()
        {
            fieldNames = new string[0];
            fieldTypes = new Datatype[0];
        }
        public CSVFields(string fieldString)
        {
            string[] fields = fieldString.Split(',');
            fieldNames = new string[fields.Length];
            fieldTypes = new Datatype[Count];
            for (int i = 0; i < Count; i++)
            {
                string[] segments = fields[i].Split(':');
                fieldNames[i] = segments[0];
                fieldTypes[i] = GetType(segments[1]);
            }
        }

        public static Datatype GetType(string typeString)
        {
            switch (typeString.ToLower())
            {
                case "int":
                case "integer":
                    return Datatype.Integer;
                case "float":
                case "number":
                    return Datatype.Number;
                case "string":
                case "text":
                default:
                    return Datatype.VarChar;
            }
        }

        public string GetFileString()
        {
            return string.Join(",", fieldNames);
        }
    }
    #endregion

    #region Record
    public class CSVRecord : Record
    {
        public CSVRecord(string valueString, int ID, Fields fields)
        {
            this.ID = ID;
            this.fields = fields;
            LoadString(valueString);
        }

        public CSVRecord(object[] values, int ID, Fields fields)
        {
            this.ID = ID;
            this.fields = fields;
            this.values = values;
        }

        public void LoadString(string valueString)
        {
            values = new object[fields.Count];
            string[] parts = new string[fields.Count];
            int currentPartIndex = 0;
            string currentPart = "";
            bool nonQuote = true;
            bool inQuote = false;
            foreach (char character in valueString)
            {
                if (character == ',' && !inQuote)
                {
                    nonQuote = true;
                    parts[currentPartIndex++] = currentPart;
                    currentPart = "";
                }
                else if (character == '"' && !inQuote) { inQuote = true; nonQuote = false; }
                else if (character == '"' && inQuote) { inQuote = false; nonQuote = true; }
                else if (inQuote || nonQuote) currentPart += character;
            }
            try
            {
                parts[currentPartIndex++] = currentPart;
                currentPart = "";
            }
            catch (IndexOutOfRangeException) { }

            for (int i = 0; i < fields.Count; i++)
            {
                switch (fields.fieldTypes[i])
                {
                    case Datatype.Number:
                        values[i] = Convert.ToDouble(parts[i]);
                        break;
                    case Datatype.Integer:
                        values[i] = Convert.ToInt32(parts[i]);
                        break;
                    case Datatype.VarChar:
                        values[i] = Convert.ToString(parts[i]);
                        break;
                }
            }
        }

        public string GetFileString()
        {
            string fileString = "";
            for (int i = 0; i < fields.Count; i++)
            {
                switch (fields.fieldTypes[i])
                {
                    case Datatype.Number:
                        fileString += Convert.ToString((double)values[i]);
                        break;
                    case Datatype.Integer:
                        fileString += Convert.ToString((int)values[i]);
                        break;
                    case Datatype.VarChar:
                        fileString += string.Format("\"{0}\"", Convert.ToString((string)values[i]));
                        break;
                }
                fileString += ",";
            }
            if (fields.Count > 0) fileString = fileString.Remove(fileString.Length - 1);
            return fileString;
        }

        public override object GetValue(string field)
        {
            return values[Array.IndexOf(fields.fieldNames, field)];
        }

        public override void SetValue(string field, object value)
        {
            if (value != null)
            {
                int fieldIndex = Array.IndexOf(fields.fieldNames, field);
                values[fieldIndex] = value;
                switch (fields.fieldTypes[fieldIndex])
                {
                    case Datatype.VarChar:
                        values[fieldIndex] = (string)value;
                        break;
                    case Datatype.Number:
                        values[fieldIndex] = (double)value;
                        break;
                    case Datatype.Integer:
                        values[fieldIndex] = (int)value;
                        break;
                }
            }
        }
    }
    #endregion
}
