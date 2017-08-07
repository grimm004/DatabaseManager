using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace DatabaseManager
{
    #region Database
    public class BINDatabase : Database
    {
        public BINDatabase(string name, bool createIfNotExists = true, string tableFileExtention = ".table")
        {
            this.tableFileExtention = tableFileExtention;
            if (!Directory.Exists(name) && createIfNotExists) Directory.CreateDirectory(name);
            string[] tableFiles = Directory.GetFiles(name, string.Format("*.{0}", tableFileExtention));
            tables = new List<Table>();
            foreach (string tableFile in tableFiles) tables.Add(new BINTable(tableFile));
            this.name = name;
        }
        
        public override Table GetTable(string tableName)
        {
            foreach (Table table in tables) if (table.Name == tableName) return table;
            return null;
        }

        public override void CreateTable(string tableName, Fields fields, bool ifNotExists = true)
        {
            string fileName = string.Format("{0}\\{1}{2}", name, tableName, tableFileExtention);
            if ((File.Exists(fileName) && !ifNotExists) || !File.Exists(fileName)) tables.Add(new BINTable(fileName, tableName, (BINFields)fields));
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
    
    #endregion
    
    #region Table
    public class BINTable : Table
    {
        public int recordsPerChunk = 10;

        private bool isNewFile;

        private int recordCount;
        public override int RecordCount { get { return recordCount; } }

        public BINTable(string fileName, string name, BINFields fields) : base(fileName, name, fields)
        { CurrentID = 0; RecordCache = new List<Record>(); isNewFile = true; }

        public BINTable(string fileName) : base(fileName)
        { isNewFile = false; }

        public override void LoadTable()
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                RecordCache = new List<Record>();
                Fields = new BINFields(reader);
                //int offset = fields.Size;
                //while (offset < (int)reader.BaseStream.Length)
                //{
                //    Record record = new BINRecord(reader, (BINFields)fields);
                //    records.Add(record);
                //    offset += fields.RecordSize;
                //}
            }
            UpdateProperties();
        }

        public override void SearchRecords(Action<Record> callback)
        {
            throw new NotImplementedException();
        }

        public override Record[] GetRecords()
        {
            List<Record> results = new List<Record>();

            using (var file = File.OpenRead(FileName))
            {
                long chunkSize = recordsPerChunk * Fields.RecordSize;
                file.Position = Fields.Size;
                int bytesRead;
                var buffer = new byte[chunkSize];
                while ((bytesRead = file.Read(buffer, 0, buffer.Length)) > 0) AnalyseChunk(ref results, buffer);
            }

            return results.ToArray();
        }

        public override Record[] GetRecords(string conditionField, object conditionValue)
        {
            //using (BinaryReader reader = new BinaryReader(File.Open(fileName, FileMode.Open)))
            //{
            //    int fieldID = fields.GetFieldID(conditionField);
            //    List<Record> resultRecords = new List<Record>();
            //    long fieldOffset = sizeof(int) + fields.fieldOffsets[fieldID];
            //    for (long i = fields.Size; i < reader.BaseStream.Length; i += fields.RecordSize)
            //    {
            //        reader.BaseStream.Position = i + fieldOffset;
            //        bool valid = false;
            //        switch (fields.fieldTypes[fieldID])
            //        {
            //            case Datatype.Number:
            //                valid = (float)conditionValue == (float)reader.ReadSingle();
            //                break;
            //            case Datatype.Integer:
            //                valid = (int)conditionValue == (int)reader.ReadInt32();
            //                break;
            //            case Datatype.VarChar:
            //                int stringSize = reader.ReadInt16();
            //                valid = (string)conditionValue == (string)Encoding.UTF8.GetString(reader.ReadBytes(stringSize));
            //                break;
            //        }

            //        if (valid)
            //        {
            //            reader.BaseStream.Position = i;
            //            resultRecords.Add(new BINRecord(reader, (BINFields)fields));
            //        }
            //    }

            //    return resultRecords.ToArray();
            //}
            List<Record> results = new List<Record>();

            using (var file = File.OpenRead(FileName))
            {
                long chunkSize = recordsPerChunk * Fields.RecordSize;
                file.Position = Fields.Size;
                int bytesRead;
                var buffer = new byte[chunkSize];
                while ((bytesRead = file.Read(buffer, 0, buffer.Length)) > 0) AnalyseChunk(ref results, buffer, conditionField, conditionValue);
            }

            return results.ToArray();
        }

        private void AnalyseChunk(ref List<Record> resultList, byte[] chunk, string conditionField, object conditionValue)
        {
            int position = 0;
            int fieldID = Fields.GetFieldID(conditionField);
            int fieldOffset = sizeof(int) + Fields.fieldOffsets[fieldID];
            for (int i = 0; i < chunk.Length; i += Fields.RecordSize)
            {
                position = i + fieldOffset;
                bool valid = false;
                switch (Fields.fieldTypes[fieldID])
                {
                    case Datatype.Number:
                        valid = (float)conditionValue == BitConverter.ToSingle(chunk, position);
                        position += sizeof(float);
                        break;
                    case Datatype.Integer:
                        valid = (int)conditionValue == BitConverter.ToInt32(chunk, position);
                        position += sizeof(int);
                        break;
                    case Datatype.VarChar:
                        int stringSize = BitConverter.ToInt16(chunk, position);
                        position += sizeof(short);
                        valid = (string)conditionValue == Encoding.UTF8.GetString(chunk, position, stringSize);
                        position += BINFields.VarCharLength;
                        break;
                }

                if (valid)
                {
                    position = i;
                    resultList.Add(new BINRecord(chunk, (BINFields)Fields, position));
                }
            }
        }

        private void AnalyseChunk(ref List<Record> resultList, byte[] chunk)
        {
            for (int i = 0; i < chunk.Length; i += Fields.RecordSize) resultList.Add(new BINRecord(chunk, (BINFields)Fields, i));
        }

        public override Record GetRecord(string conditionField, object conditionValue)
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                int fieldID = Fields.GetFieldID(conditionField);
                List<Record> resultRecords = new List<Record>();
                long fieldOffset = sizeof(int) + Fields.fieldOffsets[fieldID];
                for (long i = Fields.Size; i < reader.BaseStream.Length; i += Fields.RecordSize)
                {
                    reader.BaseStream.Position = i + fieldOffset;
                    bool valid = false;
                    switch (Fields.fieldTypes[fieldID])
                    {
                        case Datatype.Number:
                            valid = (float)conditionValue == (float)reader.ReadSingle();
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
                        return new BINRecord(reader, (BINFields)Fields);
                    }
                }

                return null;
            }
        }

        public override Record GetRecordByID(int ID)
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                //long tableSize = reader.BaseStream.Length;
                //for (int i = fields.Size; i < tableSize; i += fields.RecordSize)
                //{
                //    reader.BaseStream.Position = i;
                //    int currentID = reader.ReadInt32();
                //    if (currentID == ID)
                //    {
                //        reader.BaseStream.Position = i;
                //        return new BINRecord(reader, (BINFields)fields);
                //    }
                //}
                int pos = Fields.Size + (Fields.RecordSize * ID);
                if (pos < reader.BaseStream.Length)
                {
                    reader.BaseStream.Position = pos;
                    return new BINRecord(reader, (BINFields)Fields);
                }
            }
            return null;
        }

        public override Record AddRecord(object[] values, bool ifNotExists = false, string conditionField = null, object conditionValue = null)
        {
            MarkForUpdate();
            if ((!ifNotExists || conditionField == null || conditionValue == null)
                ||
                (ifNotExists && conditionField != null && conditionValue != null
                    && !RecordExists(conditionField, conditionValue)))
            {
                BINRecord newRecord = new BINRecord(values, recordCount++, (BINFields)Fields);
                RecordCache.Add(newRecord);
                return newRecord;
            }
            return null;
        }

        public override Record UpdateRecord(Record record, object[] values)
        {
            MarkForUpdate();
            for (int i = 0; i < FieldCount; i++) record.SetValue(Fields.fieldNames[i], values[i]);
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

        public int CurrentID { get; private set; }

        public void UpdateProperties()
        {
            using (BinaryReader reader = new BinaryReader(File.Open(FileName, FileMode.Open)))
            {
                recordCount = (int)((reader.BaseStream.Length - (long)Fields.Size) / Fields.RecordSize);
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
                    if (isNewFile) ((BINFields)Fields).WriteManifestBytes(writer);
                    foreach (BINRecord record in RecordCache) record.WriteFileBytes(writer);
                }
                isNewFile = false;
                RecordCache = new List<Record>();
                UpdateProperties();
            }
        }
    }
    #endregion

    #region Fields
    public class BINFields : Fields
    {
        public const int FieldNameSize = 32;
        public const int VarCharLength = 1024;

        public BINFields()
        {
            fieldNames = new string[0];
            fieldTypes = new Datatype[0];
            LoadTypeSizes();
        }

        public BINFields(string[] fieldNames, Datatype[] fieldTypes)
        {
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
            LoadTypeSizes();
        }

        public BINFields(BinaryReader reader)
        {
            int manifestSize = reader.ReadInt32();

            int offset = 0;

            List<string> strings = new List<string>();
            List<Datatype> types = new List<Datatype>();

            while (offset < manifestSize)
            {
                byte[] stringBytesRaw = reader.ReadBytes(FieldNameSize);
                List<byte> stringBytes = new List<byte>();
                foreach (byte currentByte in stringBytesRaw) if (currentByte != 0) stringBytes.Add(currentByte);
                strings.Add(Encoding.UTF8.GetString(stringBytes.ToArray()));
                offset += FieldNameSize;
                types.Add((Datatype)reader.ReadByte());
                offset += sizeof(byte);
            }

            Size = offset + sizeof(int);

            fieldNames = strings.ToArray();
            fieldTypes = types.ToArray();

            LoadTypeSizes();
        }

        public void LoadTypeSizes()
        {
            fieldSizes = new int[Count];
            fieldOffsets = new int[Count];
            int currentOffset = 0;
            for (int i = 0; i < Count; i++)
            {
                fieldOffsets[i] = currentOffset;
                int currentFieldSize = GetTypeSize(fieldTypes[i]);
                currentOffset += currentFieldSize;
                fieldSizes[i] = currentFieldSize;
            }
            RecordSize = sizeof(int);
            foreach (int fieldSize in fieldSizes) RecordSize += fieldSize;
        }

        public int GetTypeSize(Datatype type)
        {
            switch (type)
            {
                case Datatype.Number:
                    return sizeof(float);
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
            writer.Write((Count * (1 + FieldNameSize)));
            for (int i = 0; i < Count; i++)
            {
                WriteFieldName(writer, fieldNames[i]);
                writer.Write((byte)fieldTypes[i]);
            }
        }

        private void WriteFieldName(BinaryWriter writer, string fieldName)
        {
            if (fieldName.Length > FieldNameSize) fieldName = fieldName.Substring(0, FieldNameSize);
            List<byte> dataList = new List<byte>(Encoding.UTF8.GetBytes(fieldName));
            for (int i = dataList.Count; i < FieldNameSize; i++) dataList.Add(0);
            writer.Write(dataList.ToArray());
        }
    }
    #endregion

    #region Record
    public class BINRecord : Record
    {
        public int Size { get; private set; }

        public BINRecord(object[] values, int ID, BINFields fields)
        {
            this.ID = ID;
            this.fields = fields;
            this.values = values;
            Size = sizeof(int);
            foreach (int fieldSize in fields.fieldSizes) Size += fieldSize;
        }

        public BINRecord(byte[] data, BINFields fields, int startPosition = 0)
        {
            this.fields = fields;
            LoadRecord(data, startPosition);
            Size = sizeof(int);
            foreach (int fieldSize in fields.fieldSizes) Size += fieldSize;
        }

        public BINRecord(BinaryReader reader, BINFields fields)
        {
            this.fields = fields;
            LoadRecord(reader);
            Size = sizeof(int);
            foreach (int fieldSize in fields.fieldSizes) Size += fieldSize;
        }

        public void LoadRecord(BinaryReader reader)
        {
            this.ID = reader.ReadInt32();
            this.values = new object[fields.Count];
            for (int i = 0; i < fields.Count; i++)
            {
                switch (fields.fieldTypes[i])
                {
                    case Datatype.Number:
                        float current = reader.ReadSingle();
                        values[i] = current;
                        break;
                    case Datatype.Integer:
                        values[i] = reader.ReadInt32();
                        break;
                    case Datatype.VarChar:
                        int varCharSize = reader.ReadInt16();
                        values[i] = Encoding.UTF8.GetString(reader.ReadBytes(varCharSize));
                        reader.BaseStream.Position += BINFields.VarCharLength - varCharSize;
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
                switch (fields.fieldTypes[i])
                {
                    case Datatype.Number:
                        values[i] = BitConverter.ToSingle(data, position);
                        position += sizeof(float);
                        break;
                    case Datatype.Integer:
                        values[i] = BitConverter.ToInt32(data, position);
                        position += sizeof(int);
                        break;
                    case Datatype.VarChar:
                        int varCharSize = BitConverter.ToInt16(data, position);
                        position += sizeof(short);
                        values[i] = Encoding.UTF8.GetString(data, position, varCharSize);
                        position += BINFields.VarCharLength;
                        break;
                }
            }
        }

        public void WriteFileBytes(BinaryWriter writer)
        {
            writer.Write(ID);
            for (int i = 0; i < fields.Count; i++)
            {
                switch (fields.fieldTypes[i])
                {
                    case Datatype.Number:
                        writer.Write((float)values[i]);
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
            if (value.Length > BINFields.VarCharLength) value = value.Substring(0, BINFields.VarCharLength);
            byte[] stringBytes = Encoding.UTF8.GetBytes(value);
            writer.Write((short)stringBytes.Length);
            writer.Write(stringBytes);
            int offset = stringBytes.Length;
            for (int i = offset; i < BINFields.VarCharLength; i++) writer.Write((byte)0);
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
                        values[fieldIndex] = (float)value;
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
