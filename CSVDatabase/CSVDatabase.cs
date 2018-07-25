using System;
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

        /// <summary>
        /// Convert the database to a new binary database.
        /// </summary>
        /// <param name="name">The name of the new database.</param>
        /// <param name="varCharSizes">The sizes of each var char field in the database.</param>
        /// <param name="recordBufferSizes">The number of records to load each iteration.</param>
        /// <param name="createIfNotExists">Create the database if it does not exist.</param>
        /// <param name="tableFileExtention">The file extention for the tables.</param>
        /// <param name="updateCommand">The command to run each update.</param>
        /// <returns>the newly created database</returns>
        public BINDatabase ToBINDatabase(string name, List<ushort[]> varCharSizes, List<uint> recordBufferSizes,
            bool createIfNotExists = true, string tableFileExtention = ".table", Action<Table, double> updateCommand = null)
        {
            // The create an instance for the new database
            BINDatabase newDatabase = new BINDatabase(name, createIfNotExists, tableFileExtention);
            // Loop through each table
            for (int i = 0; i < TableCount; i++)
                // Cast the table to a CSVTable and convert it to a BINTable. Add it to the new database.
                newDatabase.AddTable(((CSVTable)Tables[i]).ToBINTable(string.Format("{0}\\{1}{2}", newDatabase.Name,
                    Tables[i].Name, tableFileExtention), Tables[i].Name, varCharSizes[i], recordBufferSizes[i], updateCommand));
            // Return the new database
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

        /// <summary>
        /// Convert the table to a binary table.
        /// </summary>
        /// <param name="fileName">The filename of the new table.</param>
        /// <param name="name">The name of the new table.</param>
        /// <param name="varCharSizes">The sizes of the var char fields.</param>
        /// <param name="recordBufferSize">The number of records to load at a time.</param>
        /// <param name="updateCommand">The command to run each update.</param>
        /// <returns>the newly converted binary table</returns>
        public BINTable ToBINTable(string fileName, string name, ushort[] varCharSizes,
            uint recordBufferSize = 100, Action<Table, double> updateCommand = null)
        {
            // Convert the CSV fields to binary fields
            BINTableFields fields = ((CSVTableFields)Fields).ToBINTableFields(varCharSizes);
            // Create a new binary table instance
            BINTable newTable = new BINTable(fileName, name, fields);
            // Create a buffer list of records
            List<Record> records = new List<Record>();
            // Define a variable to store the current CSV line
            string currentLine;
            // Define a variable to store the current record ID
            uint currentRecordId = 0;
            Console.WriteLine("Starting creation of table '{0}' ('{1}') with record buffer size {2}.",
                name, fileName, recordBufferSize);
            // Open a new stream reader
            using (StreamReader sr = new StreamReader(FileName))
            {
                // Skip the first line (the field definition line)
                sr.ReadLine();
                // While there is another line to read
                while ((currentLine = sr.ReadLine()) != "" && currentLine != null)
                {
                    // Create the record from the current line, convert it to a binary recrord
                    // Add this to the binary table
                    newTable.AddRecord(new CSVRecord(currentLine, currentRecordId++,
                        Fields).ToBINRecord(fields));
                    // If it is time to update the table file
                    if (currentRecordId % recordBufferSize == 0)
                    {
                        Console.WriteLine("Updating Table File (Current ID {0}/{1}) ({2:0.0}%)...",
                            currentRecordId, RecordCount, 100d * currentRecordId / RecordCount);
                        // Save the table to disk
                        newTable.Save();
                        // Run the update command
                        updateCommand?.Invoke(this, (double)currentRecordId / RecordCount);
                    }
                }
                // Save the table one list time in case there are unsaved changes
                newTable.Save();
                // Run the update command one last time
                updateCommand?.Invoke(this, 100);
            }
            // Return the newly created table
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

        /// <summary>
        /// Convert to binary table fields
        /// </summary>
        /// <param name="varCharSizes">the sizes of the var chars</param>
        /// <returns>the newly converted binary table fields</returns>
        public BINTableFields ToBINTableFields(ushort[] varCharSizes)
        {
            // Initialise a new array of binary field objects
            BINField[] binFields = new BINField[Count];
            // Loop through each field
            for (int i = 0; i < binFields.Length; i++)
            {
                // Convert the corresponding field to a binary field
                binFields[i] = ((CSVField)Fields[i]).ToBINField();
                // If the datatype of the current field is a VarChar
                if (binFields[i].DataType == Datatype.VarChar)
                    // Set the varchar to the specified size
                    binFields[i].VarCharSize = varCharSizes[i];
            }
            // Return a new BINTableFields object containing the new
            // binary fields
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

        /// <summary>
        /// Conver to a binary record
        /// </summary>
        /// <param name="fields">The table's fields</param>
        /// <returns>the converted binary record</returns>
        public BINRecord ToBINRecord(BINTableFields fields)
        {
            // Return a new binary record instance
            return new BINRecord(Values, ID, fields);
        }
    }
}
