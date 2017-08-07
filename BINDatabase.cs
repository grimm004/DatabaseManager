using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace DatabaseManager
{
    public class BINDatabase : Database
    {
        public BINDatabase(string name, bool createIfNotExists = true, string tableFileExtention = ".table")
        {
            this.tableFileExtention = tableFileExtention;
            if (!Directory.Exists(name) && createIfNotExists) Directory.CreateDirectory(name);
            string[] tableFiles = Directory.GetFiles(name, string.Format("*{0}", tableFileExtention));
            tables = new List<Table>();
            foreach (string tableFile in tableFiles) tables.Add(new BINTable(tableFile));
            this.name = name;
        }
        
        public override Table GetTable(string tableName)
        {
            foreach (Table table in tables) if (table.Name == tableName) return table;
            return null;
        }

        public override void CreateTable(string tableName, TableFields fields, bool ifNotExists = true)
        {
            string fileName = string.Format("{0}\\{1}{2}", name, tableName, tableFileExtention);
            if ((File.Exists(fileName) && !ifNotExists) || !File.Exists(fileName)) tables.Add(new BINTable(fileName, tableName, (BINTableFields)fields));
        }

        public override void DeleteTable(string tableName)
        {
            foreach (Table table in tables) if (table.Name == tableName) tables.Remove(table);
        }
        
        public override Record AddRecord(string tableName, object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            foreach (Table table in tables) if (table.Name == tableName) return table.AddRecord(values, ifNotExists, conditionField, conditionValue);
            return null;
        }

        public override Record GetRecordByID(string tableName, int ID)
        {
            foreach (Table table in tables) if (table.Name.ToLower() == tableName.ToLower()) return table.GetRecordByID(ID);
            return null;
        }

        public override Record[] GetRecords(string tableName, string conditionField, object conditionValue)
        {
            foreach (Table table in tables) if (table.Name == tableName) return table.GetRecords(conditionField, conditionValue);
            return null;
        }

        public override Record GetRecord(string tableName, string conditionField, object conditionValue)
        {
            foreach (Table table in tables) if (table.Name == tableName) return table.GetRecord(conditionField, conditionValue);
            return null;
        }

        public override Record UpdateRecord(string tableName, Record record, object[] values)
        {
            foreach (Table table in tables) if (table.Name == tableName) return table.UpdateRecord(record, values);
            return null;
        }

        public override Record UpdateRecord(string tableName, Record record, string fieldString, object[] value)
        {
            foreach (Table table in tables) if (table.Name == tableName) return table.UpdateRecord(record, fieldString, value);
            return null;
        }

        public override Record[] UpdateRecords(string tableName, string fieldString, object[] values, string conditionField, object conditionValue)
        {
            foreach (Table table in tables) if (table.Name == tableName) return table.UpdateRecords(fieldString, values, conditionField, conditionValue);
            return null;
        }

        public override Record UpdateRecord(string tableName, int ID, object[] values)
        {
            foreach (Table table in tables) if (table.Name == tableName) return table.UpdateRecord(ID, values);
            return null;
        }

        public override string ToString()
        {
            string tableList = "";
            foreach (Table table in tables) tableList += string.Format("'{0}', ", table.Name);
            if (TableCount > 0) tableList = tableList.Remove(tableList.Length - 2, 2);
            return string.Format("Database('{0}', {1} {2} ({3}))", name, TableCount, (TableCount == 1) ? "table" : "tables", tableList);
        }
    }
    
    public class BINTable : Table
    {
        public BINTableFields BINTableFields { get { return (BINTableFields)Fields; } }

        public int recordsPerChunk = 10;

        private bool isNewFile;

        private int recordCount;
        public override int RecordCount { get { return recordCount; } }
        public int CurrentID { get; protected set; }

        public BINTable(string fileName, string name, BINTableFields fields) : base(fileName, name, fields)
        { CurrentID = 0; RecordCache = new List<Record>(); isNewFile = true; }
        public BINTable(string fileName) : base(fileName)
        { isNewFile = false; }
        public override void LoadTable()
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                RecordCache = new List<Record>();
                Fields = new BINTableFields(reader);
            }
            UpdateProperties();
        }
        
        public override Record GetRecordByID(int ID)
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                int pos = BINTableFields.Size + (BINTableFields.RecordSize * ID);
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
                long fieldOffset = sizeof(int) + BINTableFields.BINFields[fieldID].Offset;
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
            int position = 0;
            int fieldID = BINTableFields.GetFieldID(conditionField);
            int fieldOffset = sizeof(int) + BINTableFields.BINFields[fieldID].Offset;
            for (int i = 0; i < chunk.Length; i += BINTableFields.RecordSize)
            {
                position = i + fieldOffset;
                bool valid = false;
                switch (BINTableFields.Fields[fieldID].DataType)
                {
                    case Datatype.Number:
                        valid = Convert.ToDouble(conditionValue) == BitConverter.ToDouble(chunk, position);
                        position += sizeof(double);
                        break;
                    case Datatype.Integer:
                        valid = (int)conditionValue == BitConverter.ToInt32(chunk, position);
                        position += sizeof(int);
                        break;
                    case Datatype.VarChar:
                        int stringSize = BitConverter.ToInt16(chunk, position);
                        position += sizeof(short);
                        valid = (string)conditionValue == Encoding.UTF8.GetString(chunk, position, stringSize);
                        position += BINTableFields.VarCharLength;
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
            for (int i = 0; i < chunk.Length; i += BINTableFields.RecordSize) resultList.Add(new BINRecord(chunk, BINTableFields, i));
        }
        private void AnalyseChunk(byte[] chunk, Action<Record> callback)
        {
            for (int i = 0; i < chunk.Length; i += BINTableFields.RecordSize) callback?.Invoke(new BINRecord(chunk, BINTableFields, i));
        }

        public override Record AddRecord(object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            MarkForUpdate();
            if ((!ifNotExists || conditionField == null || conditionValue == null)
                ||
                (ifNotExists && conditionField != null && conditionValue != null
                    && !RecordExists(conditionField, conditionValue)))
            {
                BINRecord newRecord = new BINRecord(values, recordCount++, BINTableFields);
                RecordCache.Add(newRecord);
                return newRecord;
            }
            return null;
        }

        public override Record UpdateRecord(Record record, object[] values)
        {
            MarkForUpdate();
            for (int i = 0; i < FieldCount; i++) record.SetValue(BINTableFields.Fields[i].Name, values[i]);
            return record;
        }
        public override Record UpdateRecord(Record record, string fieldString, object value)
        {
            MarkForUpdate();
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

        public void UpdateProperties()
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                recordCount = (int)((reader.BaseStream.Length - (long)BINTableFields.Size) / BINTableFields.RecordSize);
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
                Edited = false;
                using (BinaryWriter writer = new BinaryWriter(File.Open(FileName, FileMode.Append)))
                {
                    writer.BaseStream.Position = writer.BaseStream.Length;
                    if (isNewFile) BINTableFields.WriteManifestBytes(writer);
                    foreach (BINRecord record in RecordCache) record.WriteFileBytes(writer);
                }
                isNewFile = false;
                RecordCache = new List<Record>();
                UpdateProperties();
            }
        }
    }
    
    public class BINTableFields : TableFields
    {
        public BINField[] BINFields { get { return Array.ConvertAll(Fields, item => (BINField)item); } }

        public int Size { get; protected set; }
        public int RecordSize { get; protected set; }

        public const int FieldSize = 32;
        public const int VarCharLength = 1024;

        public BINTableFields()
        {
            Fields = new Field[0];
            LoadTypeSizes();
        }
        public BINTableFields(string[] fieldNames, Datatype[] fieldTypes)
        {
            Fields = new Field[fieldNames.Length];
            for (int i = 0; i < Fields.Length; i++)
                Fields[i] = new BINField(fieldNames[i], fieldTypes[i]);
            LoadTypeSizes();
        }
        public BINTableFields(BinaryReader reader)
        {
            int manifestSize = reader.ReadInt32();

            int offset = 0;

            List<Field> fields = new List<Field>();

            while (offset < manifestSize)
            {
                Field currentField = new BINField();
                byte[] stringBytesRaw = reader.ReadBytes(FieldSize);
                List<byte> stringBytes = new List<byte>();
                foreach (byte currentByte in stringBytesRaw) if (currentByte != 0x00) stringBytes.Add(currentByte);
                currentField.Name = Encoding.UTF8.GetString(stringBytes.ToArray());
                offset += FieldSize;
                currentField.DataType = (Datatype)reader.ReadByte();
                fields.Add(currentField);
                offset += sizeof(byte);
            }

            Size = offset + sizeof(int);

            Fields = fields.ToArray();

            LoadTypeSizes();
        }

        public void LoadTypeSizes()
        {
            int currentOffset = 0;
            for (int i = 0; i < Count; i++)
            {
                BINFields[i].Offset = currentOffset;
                int currentFieldSize = GetTypeSize(Fields[i].DataType);
                currentOffset += currentFieldSize;
                BINFields[i].Size = currentFieldSize;
            }
            RecordSize = sizeof(int);
            foreach (BINField field in Fields) RecordSize += field.Size;
        }

        public int GetTypeSize(Datatype type)
        {
            switch (type)
            {
                case Datatype.Number:
                    return sizeof(double);
                case Datatype.Integer:
                    return sizeof(int);
                case Datatype.VarChar:
                    return VarCharLength + 2;
                default:
                    return 0;
            }
        }

        public void WriteManifestBytes(BinaryWriter writer)
        {
            writer.Write((Count * (1 + FieldSize)));
            for (int i = 0; i < Count; i++)
            {
                WriteFieldName(writer, Fields[i].Name);
                writer.Write((byte)Fields[i].DataType);
            }
        }

        private void WriteFieldName(BinaryWriter writer, string fieldName)
        {
            if (fieldName.Length > FieldSize) fieldName = fieldName.Substring(0, FieldSize);
            List<byte> dataList = new List<byte>(Encoding.UTF8.GetBytes(fieldName));
            for (int i = dataList.Count; i < FieldSize; i++) dataList.Add(0);
            writer.Write(dataList.ToArray());
        }
    }

    public class BINField : Field
    {
        public int Size { get; set; }
        public int Offset { get; set; }

        public BINField() : base() { Size = 0; Offset = 0; }
        public BINField(string name, Datatype dataType) : base(name, dataType) { Size = 0; Offset = 0; }
        public BINField(string name, Datatype dataType, int size, int offset) : base(name, dataType) { Size = size; Offset = offset; }
    }

    public class BINRecord : Record
    {
        public int Size { get; private set; }

        public BINRecord(object[] values, int ID, BINTableFields fields)
        {
            this.ID = ID;
            this.fields = fields;
            this.values = values;
            Size = sizeof(int);
            foreach (BINField field in fields.Fields) Size += field.Size;
        }

        public BINRecord(byte[] data, BINTableFields fields, int startPosition = 0)
        {
            this.fields = fields;
            LoadRecord(data, startPosition);
            Size = sizeof(int);
            foreach (BINField field in fields.Fields) Size += field.Size;
        }

        public BINRecord(BinaryReader reader, BINTableFields fields)
        {
            this.fields = fields;
            LoadRecord(reader);
            Size = sizeof(int);
            foreach (BINField field in fields.Fields) Size += field.Size;
        }

        public void LoadRecord(BinaryReader reader)
        {
            this.ID = reader.ReadInt32();
            this.values = new object[fields.Count];
            for (int i = 0; i < fields.Count; i++)
            {
                switch (fields.Fields[i].DataType)
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
                        reader.BaseStream.Position += BINTableFields.VarCharLength - varCharSize;
                        break;
                }
            }
        }

        public void LoadRecord(byte[] data, int startPosition = 0)
        {
            int position = startPosition;
            this.ID = BitConverter.ToInt32(data, position);
            position += sizeof(int);
            this.values = new object[fields.Count];
            for (int i = 0; i < fields.Count; i++)
            {
                switch (fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        values[i] = BitConverter.ToDouble(data, position);
                        position += sizeof(double);
                        break;
                    case Datatype.Integer:
                        values[i] = BitConverter.ToInt32(data, position);
                        position += sizeof(int);
                        break;
                    case Datatype.VarChar:
                        int varCharSize = BitConverter.ToInt16(data, position);
                        position += sizeof(short);
                        values[i] = Encoding.UTF8.GetString(data, position, varCharSize);
                        position += BINTableFields.VarCharLength;
                        break;
                }
            }
        }

        public void WriteFileBytes(BinaryWriter writer)
        {
            writer.Write(ID);
            for (int i = 0; i < fields.Count; i++)
            {
                switch (fields.Fields[i].DataType)
                {
                    case Datatype.Number:
                        writer.Write(Convert.ToDouble(values[i]));
                        break;
                    case Datatype.Integer:
                        writer.Write((int)values[i]);
                        break;
                    case Datatype.VarChar:
                        WriteVarCharBytes(writer, (string)values[i]);
                        break;
                }
            }
        }

        public void WriteVarCharBytes(BinaryWriter writer, string value)
        {
            if (value.Length > BINTableFields.VarCharLength) value = value.Substring(0, BINTableFields.VarCharLength);
            byte[] stringBytes = Encoding.UTF8.GetBytes(value);
            writer.Write((short)stringBytes.Length);
            writer.Write(stringBytes);
            int offset = stringBytes.Length;
            for (int i = offset; i < BINTableFields.VarCharLength; i++) writer.Write((byte)0);
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
                int fieldIndex = Array.IndexOf(fields.Fields, field);
                values[fieldIndex] = value;
                switch (fields.Fields[fieldIndex].DataType)
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
                }
            }
        }
    }
}
