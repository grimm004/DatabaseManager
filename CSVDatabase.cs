using System;
using System.Collections.Generic;
using System.IO;

namespace DatabaseManager
{
    public class CSVDatabase : Database
    {
        public CSVDatabase(string name, bool createIfNotExists = true, string tableFileExtention = ".table")
        {
            this.tableFileExtention = tableFileExtention;
            if (!Directory.Exists(name) && createIfNotExists) Directory.CreateDirectory(name);
            string[] tableFiles = Directory.GetFiles(name, string.Format("*{0}", tableFileExtention));
            Tables = new List<Table>();
            foreach (string tableFile in tableFiles) Tables.Add(new CSVTable(tableFile));
            this.Name = name;
        }

        public override Table GetTable(string tableName)
        {
            foreach (Table table in Tables) if (table.Name == tableName) return table;
            return null;
        }
        public override void CreateTable(string tableName, TableFields fields, bool ifNotExists = true)
        {
            string fileName = string.Format("{0}\\{1}{2}", Name, tableName, tableFileExtention);
            if ((File.Exists(fileName) && !ifNotExists) || !File.Exists(fileName)) Tables.Add(new CSVTable(fileName, tableName, (CSVTableFields)fields));
        }
        public override void DeleteTable(string tableName)
        {
            foreach (Table table in Tables) if (table.Name == tableName) Tables.Remove(table);
        }

        public override Record GetRecordByID(string tableName, uint ID)
        {
            foreach (Table table in Tables) if (table.Name.ToLower() == tableName.ToLower()) return table.GetRecordByID(ID);
            return null;
        }
        public override Record GetRecord(string tableName, string conditionField, object conditionValue)
        {
            foreach (Table table in Tables) if (table.Name == tableName) return table.GetRecords(conditionField, conditionValue)[0];
            return null;
        }
        public override Record[] GetRecords(string tableName, string conditionField, object conditionValue)
        {
            foreach (Table table in Tables) if (table.Name == tableName) return table.GetRecords(conditionField, conditionValue);
            return null;
        }

        public override Record AddRecord(string tableName, object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            foreach (Table table in Tables) if (table.Name == tableName) return table.AddRecord(values, ifNotExists, conditionField, conditionValue);
            return null;
        }
        public override void UpdateRecord(string tableName, Record record, object[] values)
        {
            foreach (Table table in Tables) if (table.Name == tableName) table.UpdateRecord(record, values);
        }
        public override void DeleteRecord(string tableName, Record record)
        {
            foreach (Table table in Tables) if (table.Name == tableName) table.DeleteRecord(record);
        }

        public override string ToString()
        {
            string tableList = "";
            foreach (Table table in Tables) tableList += string.Format("'{0}', ", table.Name);
            if (TableCount > 0) tableList = tableList.Remove(tableList.Length - 2, 2);
            return string.Format("Database('{0}', {1} {2} ({3}))", Name, TableCount, (TableCount == 1) ? "table" : "tables", tableList);
        }

        public BINDatabase ToBINDatabase(string name, List<ushort[]> varCharSizes, List<uint> recordBufferSizes, bool createIfNotExists = true, string tableFileExtention = ".table")
        {
            BINDatabase newDatabase = new BINDatabase(name, createIfNotExists, tableFileExtention);
            for (int i = 0; i < TableCount; i++)
                newDatabase.AddTable(((CSVTable)Tables[i]).ToBINTable(string.Format("{0}\\{1}{2}", newDatabase.Name, Tables[i].Name, tableFileExtention), Tables[i].Name, varCharSizes[i], recordBufferSizes[i]));
            return newDatabase;
        }
    }
    
    public class CSVTable : Table
    {
        public CSVTable(string fileName, string name, CSVTableFields fields) : base(fileName, name, fields)
        { }
        public CSVTable(string fileName) : base(fileName)
        { }
        public override void LoadTable()
        {
            string fieldData;
            using (StreamReader sr = new StreamReader(FileName)) fieldData = sr.ReadLine();
            if (fieldData != null && fieldData.Contains(":"))
            {
                Fields = new CSVTableFields(fieldData);
                Changes = new ChangeCache();
            }
            else throw new InvalidHeaderException();
        }

        public override uint RecordCount
        {
            get
            {
                uint lineCount = 0;
                using (StreamReader sr = new StreamReader(FileName))
                    while (!sr.EndOfStream) { sr.ReadLine(); lineCount++; }
                return lineCount;
            }
        }
        public uint GetCurrnetId()
        {
            return RecordCount;
        }

        public override Record GetRecordByID(uint ID)
        {
            string currentLine;
            uint currentRecordId = 0;
            using (StreamReader sr = new StreamReader(FileName))
            {
                sr.ReadLine();
                while ((currentLine = sr.ReadLine()) != "" && currentLine != null)
                    if (currentRecordId == ID) return new CSVRecord(currentLine, currentRecordId, Fields);
                    else currentRecordId++;
            }
            return null;
        }
        public override Record GetRecord(string conditionField, object conditionValue)
        {
            return GetRecords(conditionField, conditionValue)[0];
        }
        public override Record[] GetRecords(string conditionField, object conditionValue)
        {
            List<Record> records = new List<Record>();
            string currentLine;
            uint currentRecordId = 0;
            using (StreamReader sr = new StreamReader(FileName))
            {
                sr.ReadLine();
                while ((currentLine = sr.ReadLine()) != "" && currentLine != null && !sr.EndOfStream)
                {
                    Record record = new CSVRecord(currentLine, currentRecordId++, Fields);

                    Datatype type = Fields.GetFieldType(conditionField);
                    switch (type)
                    {
                        case Datatype.Number:
                            if ((float)record.GetValue(conditionField) == (float)conditionValue) records.Add(record);
                            break;
                        case Datatype.Integer:
                            if ((int)record.GetValue(conditionField) == (int)conditionValue) records.Add(record);
                            break;
                        case Datatype.VarChar:
                            if ((string)record.GetValue(conditionField) == (string)conditionValue) records.Add(record);
                            break;
                    }
                }
            }
            return records.ToArray();
        }
        public override Record[] GetRecords()
        {
            List<Record> records = new List<Record>();
            string currentLine;
            uint currentRecordId = 0;
            using (StreamReader sr = new StreamReader(FileName))
            {
                sr.ReadLine();
                while ((currentLine = sr.ReadLine()) != "" && currentLine != null && !sr.EndOfStream)
                    records.Add(new CSVRecord(currentLine, currentRecordId++, Fields));
            }
            return records.ToArray();
        }
        public override void SearchRecords(Action<Record> callback)
        {
            StreamReader sr = new StreamReader(FileName); sr.ReadLine();
            string currentLine;
            uint currentRecordId = 0;
            while ((currentLine = sr.ReadLine()) != "" && currentLine != null && !sr.EndOfStream)
                callback?.Invoke(new CSVRecord(currentLine, currentRecordId++, Fields));
        }

        public Record AddRecord(string valueString, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            Edited = true;
            if ((!ifNotExists || conditionField == null || conditionValue == null)
                ||
                (ifNotExists && conditionField != null && conditionValue != null
                 && !RecordExists(conditionField, conditionValue)))
            {
                Record newRecord = new CSVRecord(valueString, GetCurrnetId(), Fields);
                Changes.AddedRecords.Add(newRecord);
                return newRecord;
            }
            return null;
        }
        public override Record AddRecord(object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            Edited = true;
            if ((!ifNotExists || conditionField == null || conditionValue == null)
                ||
                (ifNotExists && conditionField != null && conditionValue != null
                 && !RecordExists(conditionField, conditionValue)))
            {
                Record newRecord = new CSVRecord(values, GetCurrnetId(), Fields);
                Changes.AddedRecords.Add(newRecord);
                return newRecord;
            }
            return null;
        }

        public override void UpdateRecord(Record record, object[] values)
        {
            throw new NotImplementedException();
        }
        public override void DeleteRecord(Record record)
        {
            throw new NotImplementedException();
        }

        public override void MarkForUpdate()
        {
            Edited = true;
        }
        public override void Save()
        {
            if (!File.Exists(FileName))
            {
                StreamWriter sr = new StreamWriter(FileName);
                sr.WriteLine(((CSVTableFields)Fields).GetFileString());
                sr.Close();
            }

            if (Edited)
            {
                Edited = false;
                StreamWriter sr = new StreamWriter(FileName, true);
                foreach (CSVRecord record in Changes.AddedRecords) sr.WriteLine(record.GetFileString());
                Changes = new ChangeCache();
                sr.Close();
            }
        }
        public BINTable ToBINTable(string fileName, string name, ushort[] varCharSizes, uint recordBufferSize = 100)
        {
            BINTableFields fields = ((CSVTableFields)Fields).ToBINTableFields(varCharSizes);
            BINTable newTable = new BINTable(fileName, name, fields);
            List<Record> records = new List<Record>();
            string currentLine;
            uint currentRecordId = 0;
            Console.WriteLine("Starting creation of table '{0}' ('{1}') with record buffer size {2}.", name, fileName, recordBufferSize);
            using (StreamReader sr = new StreamReader(FileName))
            {
                sr.ReadLine();
                while ((currentLine = sr.ReadLine()) != "" && currentLine != null && !sr.EndOfStream)
                {
                    newTable.AddRecord(new CSVRecord(currentLine, currentRecordId++, Fields).ToBINRecord(fields));
                    if (currentRecordId % recordBufferSize == 0)
                    {
                        Console.WriteLine("Updating Table File (Current ID {0}/{1}) ({2:0.0}%)...", currentRecordId, RecordCount, 100 * currentRecordId / RecordCount);
                        newTable.Save();
                    }
                }
                Console.WriteLine("Updating Table File (Finalising)...");
                newTable.Save();
            }
            return newTable;
        }
    }

    public class CSVTableFields : TableFields
    {
        public CSVTableFields()
        {
            Fields = new Field[0];
        }
        public CSVTableFields(string fieldString)
        {
            string[] fields = fieldString.Split(',');
            Fields = new Field[fields.Length];
            for (int i = 0; i < Count; i++)
            {
                string[] segments = fields[i].Split(':');
                Fields[i] = new CSVField(segments[0], GetType(segments[1]));
            }
        }
        public CSVTableFields(Field[] fields)
        {
            Fields = fields;
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

        public string GetFileType(Datatype type)
        {
            switch (type)
            {
                case Datatype.Number:
                    return "number";
                case Datatype.Integer:
                    return "integer";
                case Datatype.VarChar:
                default:
                    return "string";
            }
        }

        public string GetFileString()
        {
            string fileString = "";
            for (int i = 0; i < Count; i++) fileString += string.Format("{0}:{1},", Fields[i].Name, GetFileType(Fields[i].DataType));
            return fileString.Substring(0, fileString.Length - 1);
        }

        public BINTableFields ToBINTableFields(ushort[] varCharSizes)
        {
            BINField[] binFields = new BINField[Count];
            for (int i = 0; i < binFields.Length; i++)
            {
                binFields[i] = ((CSVField)Fields[i]).ToBINField();
                if (binFields[i].DataType == Datatype.VarChar) binFields[i].VarCharSize = varCharSizes[i];
            }
            return new BINTableFields(binFields);
        }
    }

    public class CSVField : Field
    {
        public CSVField() : base() { }
        public CSVField(string name, Datatype dataType) : base(name, dataType) { }

        public BINField ToBINField()
        {
            return new BINField(Name, DataType);
        }
    }

    public class CSVRecord : Record
    {
        public CSVRecord(string valueString, uint ID, TableFields fields)
        {
            this.ID = ID;
            this.fields = fields;
            LoadString(valueString);
        }
        public CSVRecord(object[] values, uint ID, TableFields fields)
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
                switch (fields.Fields[i].DataType)
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
                switch (fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        fileString += ((double)values[i]).ToString("R");
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
            for (int i = 0; i < fields.Count; i++) if (fields.Fields[i].Name == field) return values[i];
            return null;
        }
        public override void SetValue(string field, object value)
        {
            if (value != null)
            {
                int fieldIndex = -1;
                for (int i = 0; i < fields.Count; i++) if (fields.Fields[i].Name == field) fieldIndex = i;
                values[fieldIndex] = value;
                switch (fields.Fields[fieldIndex].DataType)
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

        public BINRecord ToBINRecord(BINTableFields fields)
        {
            return new BINRecord(values, ID, fields);
        }
    }
}
