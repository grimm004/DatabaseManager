﻿using System;
using System.Collections.Generic;
using System.IO;
using DatabaseManagerLibrary.BIN;

namespace DatabaseManagerLibrary.CSV
{
    public class CSVDatabase : Database
    {
        public CSVDatabase(string name, bool createIfNotExists = true, string tableFileExtention = ".csv")
        {
            TableFileExtention = tableFileExtention;
            if (!Directory.Exists(name) && createIfNotExists) Directory.CreateDirectory(name);
            string[] tableFiles = Directory.GetFiles(name, string.Format("*{0}", tableFileExtention));
            Tables = new List<Table>();
            foreach (string tableFile in tableFiles) Tables.Add(new CSVTable(tableFile));
            Name = name;
        }
        public override Table CreateTable(string tableName, TableFields fields, bool ifNotExists = true)
        {
            string fileName = string.Format("{0}\\{1}{2}", Name, tableName, TableFileExtention);
            if ((File.Exists(fileName) && !ifNotExists) || !File.Exists(fileName))
            {
                Table table = new CSVTable(fileName, tableName, (CSVTableFields)fields);
                Tables.Add(table);
                return table;
            }
            return GetTable(tableName);
        }

        public override string ToString()
        {
            string tableList = "";
            foreach (Table table in Tables) tableList += string.Format("'{0}', ", table.Name);
            if (TableCount > 0) tableList = tableList.Remove(tableList.Length - 2, 2);
            return string.Format("Database('{0}', {1} {2} ({3}))", Name, TableCount, (TableCount == 1) ? "table" : "tables", tableList);
        }

        public BINDatabase ToBINDatabase(string name, List<ushort[]> varCharSizes, List<uint> recordBufferSizes, bool createIfNotExists = true,
            string tableFileExtention = ".table", Action<Table, double> updateCommand = null)
        {
            BINDatabase newDatabase = new BINDatabase(name, createIfNotExists, tableFileExtention);
            for (int i = 0; i < TableCount; i++)
                newDatabase.AddTable(((CSVTable)Tables[i]).ToBINTable(string.Format("{0}\\{1}{2}", newDatabase.Name,
                    Tables[i].Name, tableFileExtention), Tables[i].Name, varCharSizes[i], recordBufferSizes[i], updateCommand));
            return newDatabase;
        }
    }
    
    public class CSVTable : Table
    {
        public CSVTable(string fileName, string name, CSVTableFields fields) : base(fileName, name, fields) { }
        public CSVTable(string fileName) : base(fileName) { }
        public override void LoadTable()
        {
            string fieldData;
            using (StreamReader sr = new StreamReader(FileName)) fieldData = sr.ReadLine();
            bool validHeader = true;
            if (fieldData == null || fieldData == "")
                validHeader = false;
            else foreach (string currentField in fieldData.Split(','))
                if (!currentField.Contains(":"))
                    validHeader = false;
            if (validHeader)
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
                if (File.Exists(FileName))
                {
                    uint lineCount = 0;
                    using (StreamReader sr = new StreamReader(FileName))
                        while (!sr.EndOfStream) { sr.ReadLine(); lineCount++; }
                    return --lineCount + (uint)Changes.AddedRecords.Count - (uint)Changes.DeletedRecords.Count;
                }
                return (uint)Changes.AddedRecords.Count - (uint)Changes.DeletedRecords.Count;
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
                while ((currentLine = sr.ReadLine()) != "" && currentLine != null)
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
                        case Datatype.DateTime:
                            if ((DateTime)record.GetValue(conditionField) == (DateTime)conditionValue) records.Add(record);
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
                while ((currentLine = sr.ReadLine()) != "" && currentLine != null)
                    records.Add(new CSVRecord(currentLine, currentRecordId++, Fields));
            }
            return records.ToArray();
        }
        public override void SearchRecords(Action<Record> callback)
        {
            StreamReader sr = new StreamReader(FileName); sr.ReadLine();
            string currentLine;
            uint currentRecordId = 0;
            while ((currentLine = sr.ReadLine()) != "" && currentLine != null)
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
            MarkForUpdate();
            Changes.DeletedRecords.Add(record);
        }
        public override void DeleteRecord(uint id)
        {
            MarkForUpdate();
            Changes.DeletedRecords.Add(GetRecordByID(id));
        }

        private void FileDeleteRecord(string tempFile, CSVRecord record)
        {
            using (StreamWriter writer = new StreamWriter(tempFile, false))
                using (StreamReader reader = new StreamReader(FileName, true))
                {
                    writer.WriteLine(reader.ReadLine()); // Header Line
                    string currentLine;
                    while ((currentLine = reader.ReadLine()) != null)
                        if (String.Compare(currentLine, record.GetFileString()) != 0) writer.WriteLine(currentLine);
                }
            File.Delete(FileName);
            File.Move(tempFile, FileName);
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
                string tempFile = $"{ FileName }.temp";
                Edited = false;
                using (StreamWriter writer = new StreamWriter(FileName, true))
                    foreach (CSVRecord record in Changes.AddedRecords) writer.WriteLine(record.GetFileString());
                foreach (CSVRecord record in Changes.DeletedRecords) FileDeleteRecord(tempFile, record);
                Changes = new ChangeCache();
            }
        }
        public BINTable ToBINTable(string fileName, string name, ushort[] varCharSizes, uint recordBufferSize = 100, Action<Table, double> updateCommand = null)
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
                while ((currentLine = sr.ReadLine()) != "" && currentLine != null)
                {
                    newTable.AddRecord(new CSVRecord(currentLine, currentRecordId++, Fields).ToBINRecord(fields));
                    if (currentRecordId % recordBufferSize == 0)
                    {
                        Console.WriteLine("Updating Table File (Current ID {0}/{1}) ({2:0.0}%)...", currentRecordId, RecordCount, 100d * currentRecordId / RecordCount);
                        newTable.Save();
                        updateCommand?.Invoke(this, (double)currentRecordId / RecordCount);
                    }
                }
                //Console.WriteLine("Updating Table File (Finalising)...");
                newTable.Save();
                updateCommand?.Invoke(this, 100);
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
                case "double":
                case "real":
                case "number":
                    return Datatype.Number;
                case "datetime":
                case "date":
                    return Datatype.DateTime;
                case "string":
                case "str":
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
                case Datatype.DateTime:
                    return "datetime";
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
        public CSVField() { }
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
            this.Fields = fields;
            LoadString(valueString);
        }
        public CSVRecord(object[] values, uint ID, TableFields fields)
        {
            this.ID = ID;
            this.Fields = fields;
            this.Values = values;
        }

        public void LoadString(string valueString)
        {
            Values = new object[Fields.Count];
            string[] parts = new string[Fields.Count];
            int currentPartIndex = 0;
            string currentPart = "";
            bool nonQuote = true;
            bool inQuote = false;
            bool inEscape = false;
            foreach (char character in valueString)
            {
                if (character == ',' && !inQuote)
                {
                    nonQuote = true;
                    parts[currentPartIndex++] = currentPart;
                    currentPart = "";
                }
                else if (character == '\\' && inQuote) inEscape = true;
                else if (character == '"' && inEscape) { currentPart += character; inEscape = false; }
                else if (inEscape) { currentPart += "\\" + character; inEscape = false; }
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

            for (int i = 0; i < Fields.Count; i++)
            {
                switch (Fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        Values[i] = Convert.ToDouble(parts[i]);
                        break;
                    case Datatype.Integer:
                        Values[i] = Convert.ToInt32(parts[i]);
                        break;
                    case Datatype.VarChar:
                        Values[i] = Convert.ToString(parts[i]);
                        break;
                    case Datatype.DateTime:
                        Values[i] = DateTime.Parse(Convert.ToString(parts[i]));
                        break;
                }
            }
        }
        public string GetFileString()
        {
            string fileString = "";
            for (int i = 0; i < Fields.Count; i++)
            {
                switch (Fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        fileString += ((double)Values[i]).ToString("R");
                        break;
                    case Datatype.Integer:
                        fileString += Convert.ToString((int)Values[i]);
                        break;
                    case Datatype.VarChar:
                        fileString += string.Format("\"{0}\"", Convert.ToString((string)Values[i]).Replace("\"", "\\\""));
                        break;
                    case Datatype.DateTime:
                        fileString += ((DateTime)Values[i]).ToString("o");
                        break;
                }
                fileString += ",";
            }
            if (Fields.Count > 0) fileString = fileString.Remove(fileString.Length - 1);
            return fileString;
        }

        public BINRecord ToBINRecord(BINTableFields fields)
        {
            return new BINRecord(Values, ID, fields);
        }
    }
}
