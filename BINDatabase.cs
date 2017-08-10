using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DatabaseManager
{
    public class BINDatabase : Database
    {
        public BINDatabase(string name, bool createIfNotExists = true, string tableFileExtention = ".table")
        {
            this.TableFileExtention = tableFileExtention;
            if (!Directory.Exists(name) && createIfNotExists) Directory.CreateDirectory(name);
            string[] tableFiles = Directory.GetFiles(name, string.Format("*{0}", tableFileExtention));
            Tables = new List<Table>();
            foreach (string tableFile in tableFiles) Tables.Add(new BINTable(tableFile));
            this.Name = name;
        }
        
        public override void CreateTable(string tableName, TableFields fields, bool ifNotExists = true)
        {
            string fileName = string.Format("{0}\\{1}{2}", Name, tableName, TableFileExtention);
            if ((File.Exists(fileName) && !ifNotExists) || !File.Exists(fileName)) Tables.Add(new BINTable(fileName, tableName, (BINTableFields)fields));
        }

        public override string ToString()
        {
            string tableList = "";
            foreach (Table table in Tables) tableList += string.Format("'{0}', ", table.Name);
            if (TableCount > 0) tableList = tableList.Remove(tableList.Length - 2, 2);
            return string.Format("Database('{0}', {1} {2} ({3}))", Name, TableCount, (TableCount == 1) ? "table" : "tables", tableList);
        }
    }
    
    public class BINTable : Table
    {
        public BINTableFields BINTableFields { get { return (BINTableFields)Fields; } }

        public int recordsPerChunk = 10;

        private bool isNewFile;

        private uint recordCount;
        public override uint RecordCount { get { return recordCount; } }
        public uint CurrentID { get; protected set; }

        public BINTable(string fileName, string name, BINTableFields fields) : base(fileName, name, fields)
        { CurrentID = 0; isNewFile = true; MarkForUpdate(); }
        public BINTable(string fileName) : base(fileName)
        { isNewFile = false; }
        public override void LoadTable()
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                Changes = new ChangeCache();
                Fields = new BINTableFields(reader);
            }
            UpdateProperties();
        }
        
        public override Record GetRecordByID(uint ID)
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                uint pos = BINTableFields.Size + (BINTableFields.RecordSize * ID);
                if (pos < reader.BaseStream.Length)
                {
                    reader.BaseStream.Position = pos;
                    return new BINRecord(reader, BINTableFields);
                }
            }
            return null;
        }
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
                            valid = (int)conditionValue == (int)reader.ReadInt32();
                            break;
                        case Datatype.VarChar:
                            int stringSize = reader.ReadInt16();
                            valid = (string)conditionValue == (string)Encoding.UTF8.GetString(reader.ReadBytes(stringSize));
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
        public override Record[] GetRecords()
        {
            List<Record> results = new List<Record>();

            using (var file = File.OpenRead(FileName))
            {
                long chunkSize = recordsPerChunk * BINTableFields.RecordSize;
                file.Position = BINTableFields.Size;
                int bytesRead;
                var buffer = new byte[chunkSize];
                while ((bytesRead = file.Read(buffer, 0, buffer.Length)) > 0) AnalyseChunk(ref results, buffer);
            }

            return results.ToArray();
        }
        public override Record[] GetRecords(string conditionField, object conditionValue)
        {
            List<Record> results = new List<Record>();

            using (var file = File.OpenRead(FileName))
            {
                long chunkSize = recordsPerChunk * BINTableFields.RecordSize;
                file.Position = BINTableFields.Size;
                int bytesRead;
                var buffer = new byte[chunkSize];
                while ((bytesRead = file.Read(buffer, 0, buffer.Length)) > 0) AnalyseChunk(ref results, buffer, conditionField, conditionValue);
            }

            return results.ToArray();
        }
        public override void SearchRecords(Action<Record> callback)
        {
            using (var file = File.OpenRead(FileName))
            {
                long chunkSize = recordsPerChunk * BINTableFields.RecordSize;
                file.Position = BINTableFields.Size;
                int bytesRead;
                var buffer = new byte[chunkSize];
                while ((bytesRead = file.Read(buffer, 0, buffer.Length)) > 0) AnalyseChunk(buffer, callback);
            }
        }
        private void AnalyseChunk(ref List<Record> resultList, byte[] chunk, string conditionField, object conditionValue)
        {
            uint position = 0;
            int fieldID = BINTableFields.GetFieldID(conditionField);
            uint fieldOffset = sizeof(uint) + BINTableFields.BINFields[fieldID].Offset;
            for (uint i = 0; i < chunk.Length; i += BINTableFields.RecordSize)
            {
                position = i + fieldOffset;
                bool valid = false;
                switch (BINTableFields.Fields[fieldID].DataType)
                {
                    case Datatype.Number:
                        valid = Convert.ToDouble(conditionValue) == BitConverter.ToDouble(chunk, (int)position);
                        position += BINTableFields.BINFields[fieldID].Size;
                        break;
                    case Datatype.Integer:
                        valid = (int)conditionValue == BitConverter.ToInt32(chunk, (int)position);
                        position += sizeof(int);
                        break;
                    case Datatype.VarChar:
                        int stringSize = BitConverter.ToInt16(chunk, (int)position);
                        position += sizeof(ushort);
                        valid = (string)conditionValue == Encoding.UTF8.GetString(chunk, (int)position, stringSize);
                        position += BINTableFields.BINFields[fieldID].Size;
                        break;
                }

                if (valid)
                {
                    position = i;
                    resultList.Add(new BINRecord(chunk, BINTableFields, position));
                }
            }
        }
        private void AnalyseChunk(ref List<Record> resultList, byte[] chunk)
        {
            for (uint i = 0; i < chunk.Length; i += BINTableFields.RecordSize) resultList.Add(new BINRecord(chunk, BINTableFields, i));
        }
        private void AnalyseChunk(byte[] chunk, Action<Record> callback)
        {
            for (uint i = 0; i < chunk.Length; i += BINTableFields.RecordSize) callback?.Invoke(new BINRecord(chunk, BINTableFields, i));
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
                if (isNewFile) File.Create(FileName).Close();
                Edited = false;
                using (BinaryWriter writer = new BinaryWriter(File.Open(FileName, FileMode.Open)))
                {
                    writer.BaseStream.Position = writer.BaseStream.Length;
                    if (isNewFile || Fields.Edited) BINTableFields.WriteManifestBytes(writer);
                    foreach (BINRecord record in Changes.AddedRecords) record.WriteFileBytes(writer, false);
                    long position = writer.BaseStream.Position;
                    foreach (BINRecord record in Changes.ChangedRecords) record.WriteFileBytes(writer, true);
                    foreach (BINRecord record in Changes.DeletedRecords) record.DeleteFileBytes(writer, 100);
                }
                isNewFile = false;
                Changes = new ChangeCache();
                UpdateProperties();
            }
        }
    }
    
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

    public class BINField : Field
    {
        public uint Size { get; protected set; }
        public uint Offset { get; set; }
        public ushort VarCharSize { get { return (ushort)(Size - 2); } set { Size = value + (uint)2; } }

        //public BINField() : base() { Size = 0; Offset = 0; }
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
                default:
                    break;
            }
            Offset = 0;
        }
        //public BINField(string name, Datatype dataType, uint size, uint offset) : base(name, dataType) { Size = size; Offset = offset; }
    }

    public class BINRecord : Record
    {
        public BINRecord(object[] values, uint ID, BINTableFields fields)
        {
            this.ID = ID;
            this.Fields = fields;
            this.values = values;
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
            this.values = new object[Fields.Count];
            for (int i = 0; i < Fields.Count; i++)
            {
                switch (Fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        double current = reader.ReadDouble();
                        values[i] = current;
                        break;
                    case Datatype.Integer:
                        values[i] = reader.ReadInt32();
                        break;
                    case Datatype.VarChar:
                        int varCharSize = reader.ReadInt16();
                        values[i] = Encoding.UTF8.GetString(reader.ReadBytes(varCharSize));
                        reader.BaseStream.Position += ((BINField)Fields.Fields[i]).VarCharSize - varCharSize;
                        break;
                }
            }
        }
        public void LoadRecord(byte[] data, uint startPosition = 0)
        {
            uint position = startPosition;
            this.ID = BitConverter.ToUInt32(data, (int)position);
            position += sizeof(uint);
            this.values = new object[Fields.Count];
            for (int i = 0; i < Fields.Count; i++)
                switch (Fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        values[i] = BitConverter.ToDouble(data, (int)position);
                        position += sizeof(double);
                        break;
                    case Datatype.Integer:
                        values[i] = BitConverter.ToInt32(data, (int)position);
                        position += sizeof(int);
                        break;
                    case Datatype.VarChar:
                        int varCharSize = BitConverter.ToInt16(data, (int)position);
                        position += sizeof(ushort);
                        values[i] = Encoding.UTF8.GetString(data, (int)position, varCharSize);
                        position += ((BINField)Fields.Fields[i]).VarCharSize;
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
                        writer.Write(Convert.ToDouble(values[i]));
                        break;
                    case Datatype.Integer:
                        writer.Write((int)values[i]);
                        break;
                    case Datatype.VarChar:
                        BINField field = (BINField)Fields.Fields[i];
                        string value = (string)values[i];
                        if (value.Length > field.VarCharSize) value = value.Substring(0, field.VarCharSize);
                        byte[] stringBytes = Encoding.UTF8.GetBytes(value);
                        writer.Write((ushort)stringBytes.Length);
                        writer.Write(stringBytes);
                        uint offset = (uint)stringBytes.Length;
                        for (uint j = offset; j < field.VarCharSize; j++) writer.Write((byte)0x00);
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
