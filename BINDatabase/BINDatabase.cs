using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DatabaseManagerLibrary.BIN
{
    /// <summary>
    /// Represents a Binary database
    /// </summary>
    public class BINDatabase : Database
    {
        /// <summary>
        /// Constructor for the binary database
        /// </summary>
        /// <param name="name">The name of the database</param>
        /// <param name="createIfNotExists">Create the database if it does not already exist.</param>
        /// <param name="tableFileExtention">The file extention for tables.</param>
        public BINDatabase(string name, bool createIfNotExists = true, string tableFileExtention = ".table")
        {
            this.TableFileExtention = tableFileExtention;
            if (!Directory.Exists(name) && createIfNotExists) Directory.CreateDirectory(name);
            // Optain a list of table file names
            string[] tableFiles = Directory.GetFiles(name, string.Format("*{0}", tableFileExtention));
            Tables = new List<Table>();
            // Loop through each table file name and instanciate the table objects
            foreach (string tableFile in tableFiles) Tables.Add(new BINTable(tableFile));
            Name = name;
        }
        
        /// <summary>
        /// Create a table
        /// </summary>
        /// <param name="tableName">The name of the table to create</param>
        /// <param name="fields">The fields for the table</param>
        /// <param name="ifNotExists">Create the table if it does not exist</param>
        /// <returns>the newly created table</returns>
        public override Table CreateTable(string tableName, TableFields fields, bool ifNotExists = true)
        {
            // Get the table file name
            string fileName = string.Format("{0}\\{1}{2}", Name, tableName, TableFileExtention);
            // If the file does not exist
            if ((File.Exists(fileName) && !ifNotExists) || !File.Exists(fileName))
            {
                // Create the new table
                Table table = new BINTable(fileName, tableName, (BINTableFields)fields);
                // Add the table to the table list
                Tables.Add(table);
                // Return the newly created table
                return table;
            }
            // Return any table matching the file name
            return GetTable(tableName);
        }

        public override string ToString()
        {
            string tableList = "";
            foreach (Table table in Tables) tableList += string.Format("'{0}', ", table.Name);
            if (TableCount > 0) tableList = tableList.Remove(tableList.Length - 2, 2);
            return string.Format("Database('{0}', {1} {2} ({3}))", Name, TableCount, (TableCount == 1) ? "table" : "tables", tableList);
        }
    }

    /// <summary>
    /// Represents a Table for a Binary Database
    /// </summary>
    public class BINTable : Table
    {
        public BINTableFields BINTableFields { get { return (BINTableFields)Fields; } }

        public readonly int recordsPerChunk = 10;

        private bool IsNewFile { get; set; }

        private uint recordCount;
        public override uint RecordCount { get { return recordCount; } }
        public uint CurrentID { get; protected set; }

        /// <summary>
        /// Initialize a new table instance
        /// </summary>
        /// <param name="fileName">The filename of the table</param>
        /// <param name="name">The name of the table</param>
        /// <param name="fields">The table's fields</param>
        public BINTable(string fileName, string name, BINTableFields fields) : base(fileName, name, fields)
        { CurrentID = 0; IsNewFile = true; MarkForUpdate(); }
        /// <summary>
        /// Initialize a new table instance
        /// </summary>
        /// <param name="fileName">The filename of the table</param>
        public BINTable(string fileName) : base(fileName)
        { IsNewFile = false; }
        /// <summary>
        /// Load the table's metadata and fields
        /// </summary>
        public override void LoadTable()
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                Changes = new ChangeCache();
                Fields = new BINTableFields(reader);
            }
            UpdateProperties();
        }
        
        /// <summary>
        /// Get a record by its ID
        /// </summary>
        /// <param name="ID">The ID of the record to get.</param>
        /// <returns>the record corresponding to the ID</returns>
        public override Record GetRecordByID(uint ID)
        {
            // Open a binary file reader
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                // Calculate the position of the desired record in the file
                uint pos = BINTableFields.Size + (BINTableFields.RecordSize * ID);
                // If the target position does not exceed the length of the file
                if (pos < reader.BaseStream.Length)
                {
                    // Set the binary reader's base stream position to the calculated pos
                    reader.BaseStream.Position = pos;
                    // Return a new record produced from this position
                    return new BINRecord(reader, BINTableFields);
                }
            }
            // If unsuccsessful, return null
            return null;
        }
        /// <summary>
        /// Get a record based on a condition
        /// </summary>
        /// <param name="conditionField">The field to check the condition with</param>
        /// <param name="conditionValue">The value to compare as the condition</param>
        /// <returns>the first record matching the condition</returns>
        public override Record GetRecord(string conditionField, object conditionValue)
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                int fieldID = BINTableFields.GetFieldID(conditionField);
                List<Record> resultRecords = new List<Record>();
                long fieldOffset = sizeof(uint) + BINTableFields.BINFields[fieldID].Offset;
                for (long i = BINTableFields.Size; i < reader.BaseStream.Length; i += BINTableFields.RecordSize)
                {
                    reader.BaseStream.Position = i + fieldOffset;
                    bool valid = false;
                    switch (BINTableFields.Fields[fieldID].DataType)
                    {
                        case Datatype.Number:
                            valid = Convert.ToDouble(conditionValue) == reader.ReadDouble();
                            break;
                        case Datatype.Integer:
                            valid = (int)conditionValue == reader.ReadInt32();
                            break;
                        case Datatype.VarChar:
                            int stringSize = reader.ReadInt16();
                            valid = (string)conditionValue == Encoding.UTF8.GetString(reader.ReadBytes(stringSize));
                            break;
                        case Datatype.DateTime:
                            valid = (DateTime)conditionValue == DateTime.FromBinary(reader.ReadInt64());
                            break;
                    }

                    if (valid)
                    {
                        reader.BaseStream.Position = i;
                        return new BINRecord(reader, BINTableFields);
                    }
                }

                return null;
            }
        }
        /// <summary>
        /// Get all records
        /// </summary>
        /// <returns>all records in the table</returns>
        public override Record[] GetRecords()
        {
            // Create a list of records to store the results in
            List<Record> results = new List<Record>();

            // Open the table's file
            using (FileStream file = File.OpenRead(FileName))
            {
                // Calculate the chunk size
                long chunkSize = recordsPerChunk * BINTableFields.RecordSize;
                // Set the file position to the start of the records
                file.Position = BINTableFields.Size;
                // Define a variable to store the number of bytes read per chunk
                int bytesRead;
                // Define a buffer to store the chunk data in
                var buffer = new byte[chunkSize];
                // While there is data to be read, read it into the buffer
                while ((bytesRead = file.Read(buffer, 0, buffer.Length)) > 0)
                    // Analyse the chunk to fetch the records
                    AnalyseChunk(ref results, buffer);
            }

            return results.ToArray();
        }
        /// <summary>
        /// Get all records matching a condition
        /// </summary>
        /// <param name="conditionField">The field to check for the condition in</param>
        /// <param name="conditionValue">The condition value to compare against</param>
        /// <returns>all records matching the condtion</returns>
        public override Record[] GetRecords(string conditionField, object conditionValue)
        {
            // Create a list of records (to store the matching records)
            List<Record> results = new List<Record>();

            // Open a new file stream for the table's file
            using (FileStream file = File.OpenRead(FileName))
            {
                // Calculate the chunk size
                long chunkSize = recordsPerChunk * BINTableFields.RecordSize;
                // Set the position to the file to the start of the records
                file.Position = BINTableFields.Size;
                // Define a variable to store the number of bytes read per chunk
                int bytesRead;
                // Define a cunk buffer
                var buffer = new byte[chunkSize];
                // While there is data to be read from the file, read it into the chunk buffer
                while ((bytesRead = file.Read(buffer, 0, buffer.Length)) > 0)
                    // Analyse the cunk for records
                    AnalyseChunk(ref results, buffer, conditionField, conditionValue);
            }

            return results.ToArray();
        }
        /// <summary>
        /// Search through all the records
        /// </summary>
        /// <param name="callback">The callback to parse each record to when loaded.</param>
        public override void SearchRecords(Action<Record> callback)
        {
            // Open the table file
            using (FileStream file = File.OpenRead(FileName))
            {
                // Calculate the chunk size
                long chunkSize = recordsPerChunk * BINTableFields.RecordSize;
                // Set the file position to the start of the records
                file.Position = BINTableFields.Size;
                // Define a variable to store the number of bytes read from the file every chunk
                int bytesRead;
                // Define a buffer to store the cunk
                var buffer = new byte[chunkSize];
                // While there is data to be read from the file, read it to the chunk buffer
                while ((bytesRead = file.Read(buffer, 0, buffer.Length)) > 0)
                    // Analyse the current chunk of data
                    AnalyseChunk(buffer, callback);
            }
        }
        /// <summary>
        /// Conditionally analyse a chunk of record data
        /// </summary>
        /// <param name="resultList">A reference to the record results list</param>
        /// <param name="chunk">The cunk of data to analyse</param>
        /// <param name="conditionField">The field to check the condition against</param>
        /// <param name="conditionValue">The condition value to check for</param>
        private void AnalyseChunk(ref List<Record> resultList, byte[] chunk, string conditionField, object conditionValue)
        {
            // Initialise a chunk position variable to zero
            uint position = 0;
            // Get the field ID of the conditional field
            int fieldID = BINTableFields.GetFieldID(conditionField);
            // Calculate the field byte offset of the conditional field
            uint fieldOffset = sizeof(uint) + BINTableFields.BINFields[fieldID].Offset;
            // Loop through each record in the data chunk
            for (uint i = 0; i < chunk.Length; i += BINTableFields.RecordSize)
            {
                // Set the position in the chunk to the current loop index plus the pre-calculated field offset
                position = i + fieldOffset;
                // Define a variable to store if the record is a valid match to the condition
                bool valid = false;
                // Switch through the datatype of the conditional field
                switch (BINTableFields.Fields[fieldID].DataType)
                {
                    case Datatype.Number:
                        // Convert the data to a double and compare it to the condition value
                        valid = Convert.ToDouble(conditionValue) == BitConverter.ToDouble(chunk, (int)position);
                        position += BINTableFields.BINFields[fieldID].Size;
                        break;
                    case Datatype.Integer:
                        // Convert the field data to an integer and compare it to the condition value
                        valid = (int)conditionValue == BitConverter.ToInt32(chunk, (int)position);
                        position += sizeof(int);
                        break;
                    case Datatype.VarChar:
                        // Convert the field data to a string and compare it to the condition value
                        int stringSize = BitConverter.ToInt16(chunk, (int)position);
                        position += sizeof(ushort);
                        valid = (string)conditionValue == Encoding.UTF8.GetString(chunk, (int)position, stringSize);
                        position += BINTableFields.BINFields[fieldID].Size;
                        break;
                    case Datatype.DateTime:
                        // Convert the field data to a DateTime instance and compare it to the condition value
                        valid = (DateTime)conditionValue == DateTime.FromBinary(BitConverter.ToInt64(chunk, (int)position));
                        position += sizeof(long);
                        break;
                }

                // If the condition value matches the data in the desired field
                if (valid)
                {
                    // Re-set the position to the beginning of the record data
                    position = i;
                    // Read the record into memory and add it to the results list
                    resultList.Add(new BINRecord(chunk, BINTableFields, position));
                }
            }
        }
        /// <summary>
        /// Load all records for a chunk of data.
        /// </summary>
        /// <param name="resultList">A reference to the record list to add the loaded records to.</param>
        /// <param name="chunk">The chunk to read through</param>
        private void AnalyseChunk(ref List<Record> resultList, byte[] chunk)
        {
            // Loop through each record position in the record data chunk
            for (uint i = 0; i < chunk.Length; i += BINTableFields.RecordSize)
                // Create a record from this position in the chunk and add it to the results list
                resultList.Add(new BINRecord(chunk, BINTableFields, i));
        }
        private void AnalyseChunk(byte[] chunk, Action<Record> callback)
        {
            // Loop through each record position in the record data chunk
            for (uint i = 0; i < chunk.Length; i += BINTableFields.RecordSize)
                // Create a record from this position in the chunk and send it to the desired callback
                callback?.Invoke(new BINRecord(chunk, BINTableFields, i));
        }

        public override Record AddRecord(object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            MarkForUpdate();
            if ((!ifNotExists || conditionField == null || conditionValue == null)
                ||
                (ifNotExists && conditionField != null && conditionValue != null
                    && !RecordExists(conditionField, conditionValue)))
            {
                BINRecord newRecord = new BINRecord(values, recordCount + (uint)Changes.AddedRecords.Count, BINTableFields);
                Changes.AddedRecords.Add(newRecord);
                return newRecord;
            }
            return null;
        }

        public override void UpdateRecord(Record record, object[] values)
        {
            MarkForUpdate();
            for (int i = 0; i < FieldCount; i++) record.SetValue(Fields.Fields[i].Name, values[i]);
            Changes.ChangedRecords.Add(record);
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

        public void UpdateProperties()
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                recordCount = (uint)((reader.BaseStream.Length - BINTableFields.Size) / BINTableFields.RecordSize);
                CurrentID = recordCount - 1;
            }
        }
        public override void MarkForUpdate()
        {
            Edited = true;
        }
        public override void Save()
        {
            if (Edited)
            {
                if (IsNewFile) File.Create(FileName).Close();
                Edited = false;
                using (BinaryWriter writer = new BinaryWriter(File.Open(FileName, FileMode.Open)))
                {
                    writer.BaseStream.Position = writer.BaseStream.Length;
                    if (IsNewFile || Fields.Edited) BINTableFields.WriteManifestBytes(writer);
                    foreach (BINRecord record in Changes.AddedRecords) record.WriteFileBytes(writer, false);
                    long position = writer.BaseStream.Position;
                    foreach (BINRecord record in Changes.ChangedRecords) record.WriteFileBytes(writer, true);
                    foreach (BINRecord record in Changes.DeletedRecords) record.DeleteFileBytes(writer, 100);
                }
                IsNewFile = false;
                Changes = new ChangeCache();
                UpdateProperties();
            }
        }
    }
    
    /// <summary>
    /// Represents the collection of Fields for the Binary Database
    /// </summary>
    public class BINTableFields : TableFields
    {
        public BINField[] BINFields { get { return Array.ConvertAll(Fields, item => (BINField)item); } }

        public uint Size { get; protected set; }
        public uint RecordSize { get; protected set; }

        public const int FieldNameSize = 32;

        public BINTableFields()
        {
            Fields = new Field[0];
            Size = 0;
            LoadTypeSizes();
        }
        public BINTableFields(string[] fieldNames, Datatype[] fieldTypes, ushort[] varCharLengths)
        {
            Fields = new Field[fieldNames.Length];
            for (int i = 0; i < Fields.Length; i++)
            {
                Fields[i] = new BINField(fieldNames[i], fieldTypes[i]);
                if (fieldTypes[i] == Datatype.VarChar) BINFields[i].VarCharSize = varCharLengths[i];
            }
            LoadTypeSizes();
        }
        public BINTableFields(params BINField[] fields)
        {
            Fields = fields;
            LoadTypeSizes();
        }
        public BINTableFields(BinaryReader reader)
        {
            int manifestSize = reader.ReadInt32();
            uint offset = 0;
            List<BINField> fields = new List<BINField>();
            while (offset < manifestSize)
            {
                byte[] stringBytesRaw = reader.ReadBytes(FieldNameSize);
                List<byte> stringBytes = new List<byte>();
                foreach (byte currentByte in stringBytesRaw) if (currentByte != 0x00) stringBytes.Add(currentByte);
                string name = Encoding.UTF8.GetString(stringBytes.ToArray());
                offset += FieldNameSize;
                Datatype dataType = (Datatype)reader.ReadByte();
                offset += sizeof(byte);
                ushort varCharSize = 0;
                if (dataType == Datatype.VarChar)
                {
                    varCharSize = reader.ReadUInt16();
                    offset += sizeof(ushort);
                }
                fields.Add(new BINField(name, dataType, varCharSize));
            }
            Size = sizeof(uint) + offset;
            Fields = fields.ToArray();
            LoadTypeSizes();
        }

        public void LoadTypeSizes()
        {
            uint currentOffset = 0;
            RecordSize = sizeof(uint);
            for (int i = 0; i < Count; i++)
            {
                BINFields[i].Offset = currentOffset;
                currentOffset += BINFields[i].Size;
                RecordSize += BINFields[i].Size;
            }
        }

        public void WriteManifestBytes(BinaryWriter writer)
        {
            writer.BaseStream.Position = 0;
            int manifestSize = 0;
            foreach (BINField field in Fields)
                manifestSize += field.DataType != Datatype.VarChar ? FieldNameSize + sizeof(byte) : FieldNameSize + sizeof(byte) + sizeof(ushort);
            writer.Write(manifestSize);
            for (int i = 0; i < Count; i++)
            {
                WriteFieldName(writer, Fields[i].Name);
                writer.Write((byte)Fields[i].DataType);
                if (Fields[i].DataType == Datatype.VarChar)
                    writer.Write(BINFields[i].VarCharSize);
            }
            Edited = false;
        }

        private void WriteFieldName(BinaryWriter writer, string fieldName)
        {
            if (fieldName.Length > FieldNameSize) fieldName = fieldName.Substring(0, FieldNameSize);
            byte[] data = Encoding.UTF8.GetBytes(fieldName);
            writer.Write(data);
            for (int i = data.Length; i < FieldNameSize; i++) writer.Write((byte)0x00);
        }
    }

    /// <summary>
    /// Represents a single Field for a Binary Database
    /// </summary>
    public class BINField : Field
    {
        public uint Size { get; protected set; }
        public uint Offset { get; set; }
        public ushort VarCharSize { get { return (ushort)(Size - 2); } set { Size = value + (uint)2; } }
        
        public BINField(string name, Datatype dataType, ushort varCharSize = 0) : base(name, dataType)
        {
            switch (dataType)
            {
                case Datatype.Number:
                    Size = sizeof(double);
                    break;
                case Datatype.Integer:
                    Size = sizeof(int);
                    break;
                case Datatype.VarChar:
                    VarCharSize = varCharSize;
                    break;
                case Datatype.DateTime:
                    Size = sizeof(long);
                    break;
                default:
                    break;
            }
            Offset = 0;
        }
    }

    /// <summary>
    /// Represents a single Record for a Binary Database
    /// </summary>
    public class BINRecord : Record
    {
        public BINRecord(object[] values, uint ID, BINTableFields fields)
        {
            this.ID = ID;
            Fields = fields;
            Values = values;
        }
        public BINRecord(byte[] data, BINTableFields fields, uint startPosition = 0)
        {
            this.Fields = fields;
            LoadRecord(data, startPosition);
        }
        public BINRecord(BinaryReader reader, BINTableFields fields)
        {
            this.Fields = fields;
            LoadRecord(reader);
        }

        public void LoadRecord(BinaryReader reader)
        {
            this.ID = reader.ReadUInt32();
            this.Values = new object[Fields.Count];
            for (int i = 0; i < Fields.Count; i++)
                switch (Fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        double current = reader.ReadDouble();
                        Values[i] = current;
                        break;
                    case Datatype.Integer:
                        Values[i] = reader.ReadInt32();
                        break;
                    case Datatype.VarChar:
                        int varCharSize = reader.ReadInt16();
                        Values[i] = Encoding.UTF8.GetString(reader.ReadBytes(varCharSize));
                        reader.BaseStream.Position += ((BINField)Fields.Fields[i]).VarCharSize - varCharSize;
                        break;
                    case Datatype.DateTime:
                        Values[i] = DateTime.FromBinary(reader.ReadInt64());
                        break;
                }
        }
        public void LoadRecord(byte[] data, uint startPosition = 0)
        {
            uint position = startPosition;
            this.ID = BitConverter.ToUInt32(data, (int)position);
            position += sizeof(uint);
            this.Values = new object[Fields.Count];
            for (int i = 0; i < Fields.Count; i++)
                switch (Fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        Values[i] = BitConverter.ToDouble(data, (int)position);
                        position += sizeof(double);
                        break;
                    case Datatype.Integer:
                        Values[i] = BitConverter.ToInt32(data, (int)position);
                        position += sizeof(int);
                        break;
                    case Datatype.VarChar:
                        int varCharSize = BitConverter.ToInt16(data, (int)position);
                        position += sizeof(ushort);
                        Values[i] = Encoding.UTF8.GetString(data, (int)position, varCharSize);
                        position += ((BINField)Fields.Fields[i]).VarCharSize;
                        break;
                    case Datatype.DateTime:
                        Values[i] = DateTime.FromBinary(BitConverter.ToInt64(data, (int)position));
                        position += sizeof(long);
                        break;
                }
        }

        public void WriteFileBytes(BinaryWriter writer, bool positionAtId)
        {
            writer.BaseStream.Position = positionAtId ? ((BINTableFields)Fields).Size + (((BINTableFields)Fields).RecordSize * ID) : writer.BaseStream.Length;
            writer.Write(ID);
            for (int i = 0; i < Fields.Count; i++)
                switch (Fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        writer.Write(Convert.ToDouble(Values[i]));
                        break;
                    case Datatype.Integer:
                        writer.Write((int)Values[i]);
                        break;
                    case Datatype.VarChar:
                        BINField field = (BINField)Fields.Fields[i];
                        string value = (string)Values[i];
                        if (value.Length > field.VarCharSize) value = value.Substring(0, field.VarCharSize);
                        byte[] stringBytes = Encoding.UTF8.GetBytes(value);
                        writer.Write((ushort)stringBytes.Length);
                        writer.Write(stringBytes);
                        uint offset = (uint)stringBytes.Length;
                        for (uint j = offset; j < field.VarCharSize; j++) writer.Write((byte)0x00);
                        break;
                    case Datatype.DateTime:
                        writer.Write(((DateTime)Values[i]).Ticks);
                        break;
                }
        }
        public void DeleteFileBytes(BinaryWriter writer, int recordsPerChunk)
        {
            BINTableFields bFields = (BINTableFields)Fields;
            writer.BaseStream.Position = bFields.Size + (bFields.RecordSize * ID);
            byte[] data = new byte[((BINTableFields)Fields).RecordSize];
            for (int i = 0; i < data.Length; i++) data[i] = 0x00;
            writer.Write(data, 0, data.Length);

            int bytesRead;
            byte[] currentChunk;
            writer.BaseStream.Position = bFields.Size + (bFields.RecordSize * ID);
            do
            {
                currentChunk = new byte[recordsPerChunk * bFields.RecordSize];
                writer.BaseStream.Position += bFields.RecordSize;
                bytesRead = writer.BaseStream.Read(currentChunk, 0, Math.Min(currentChunk.Length, (int)(writer.BaseStream.Length - writer.BaseStream.Position)));
                writer.BaseStream.Position -= (currentChunk.Length + (int)bFields.RecordSize);
                writer.Write(currentChunk, 0, bytesRead);
            } while (bytesRead == currentChunk.Length);
            writer.BaseStream.SetLength(Math.Max(0, writer.BaseStream.Length - bFields.RecordSize));
        }
    }
}
