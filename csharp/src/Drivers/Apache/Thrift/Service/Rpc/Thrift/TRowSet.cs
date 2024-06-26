/**
 * <auto-generated>
 * Autogenerated by Thrift Compiler (0.17.0)
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 * </auto-generated>
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Thrift;
using Thrift.Collections;
using Thrift.Protocol;
using Thrift.Protocol.Entities;
using Thrift.Protocol.Utilities;
using Thrift.Transport;
using Thrift.Transport.Client;
using Thrift.Transport.Server;
using Thrift.Processor;


#pragma warning disable IDE0079  // remove unnecessary pragmas
#pragma warning disable IDE0017  // object init can be simplified
#pragma warning disable IDE0028  // collection init can be simplified
#pragma warning disable IDE1006  // parts of the code use IDL spelling
#pragma warning disable CA1822   // empty DeepCopy() methods still non-static
#pragma warning disable IDE0083  // pattern matching "that is not SomeType" requires net5.0 but we still support earlier versions

namespace Apache.Hive.Service.Rpc.Thrift
{

  public partial class TRowSet : TBase
  {
    private List<global::Apache.Hive.Service.Rpc.Thrift.TColumn> _columns;
    private byte[] _binaryColumns;
    private int _columnCount;
    private List<global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowBatch> _arrowBatches;
    private List<global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowResultLink> _resultLinks;

    public long StartRowOffset { get; set; }

    public List<global::Apache.Hive.Service.Rpc.Thrift.TRow> Rows { get; set; }

    public List<global::Apache.Hive.Service.Rpc.Thrift.TColumn> Columns
    {
      get
      {
        return _columns;
      }
      set
      {
        __isset.columns = true;
        this._columns = value;
      }
    }

    public byte[] BinaryColumns
    {
      get
      {
        return _binaryColumns;
      }
      set
      {
        __isset.binaryColumns = true;
        this._binaryColumns = value;
      }
    }

    public int ColumnCount
    {
      get
      {
        return _columnCount;
      }
      set
      {
        __isset.columnCount = true;
        this._columnCount = value;
      }
    }

    public List<global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowBatch> ArrowBatches
    {
      get
      {
        return _arrowBatches;
      }
      set
      {
        __isset.arrowBatches = true;
        this._arrowBatches = value;
      }
    }

    public List<global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowResultLink> ResultLinks
    {
      get
      {
        return _resultLinks;
      }
      set
      {
        __isset.resultLinks = true;
        this._resultLinks = value;
      }
    }


    public Isset __isset;
    public struct Isset
    {
      public bool columns;
      public bool binaryColumns;
      public bool columnCount;
      public bool arrowBatches;
      public bool resultLinks;
    }

    public TRowSet()
    {
    }

    public TRowSet(long startRowOffset, List<global::Apache.Hive.Service.Rpc.Thrift.TRow> rows) : this()
    {
      this.StartRowOffset = startRowOffset;
      this.Rows = rows;
    }

    public TRowSet DeepCopy()
    {
      var tmp209 = new TRowSet();
      tmp209.StartRowOffset = this.StartRowOffset;
      if ((Rows != null))
      {
        tmp209.Rows = this.Rows.DeepCopy();
      }
      if ((Columns != null) && __isset.columns)
      {
        tmp209.Columns = this.Columns.DeepCopy();
      }
      tmp209.__isset.columns = this.__isset.columns;
      if ((BinaryColumns != null) && __isset.binaryColumns)
      {
        tmp209.BinaryColumns = this.BinaryColumns.ToArray();
      }
      tmp209.__isset.binaryColumns = this.__isset.binaryColumns;
      if (__isset.columnCount)
      {
        tmp209.ColumnCount = this.ColumnCount;
      }
      tmp209.__isset.columnCount = this.__isset.columnCount;
      if ((ArrowBatches != null) && __isset.arrowBatches)
      {
        tmp209.ArrowBatches = this.ArrowBatches.DeepCopy();
      }
      tmp209.__isset.arrowBatches = this.__isset.arrowBatches;
      if ((ResultLinks != null) && __isset.resultLinks)
      {
        tmp209.ResultLinks = this.ResultLinks.DeepCopy();
      }
      tmp209.__isset.resultLinks = this.__isset.resultLinks;
      return tmp209;
    }

    public async global::System.Threading.Tasks.Task ReadAsync(TProtocol iprot, CancellationToken cancellationToken)
    {
      iprot.IncrementRecursionDepth();
      try
      {
        bool isset_startRowOffset = false;
        bool isset_rows = false;
        TField field;
        await iprot.ReadStructBeginAsync(cancellationToken);
        while (true)
        {
          field = await iprot.ReadFieldBeginAsync(cancellationToken);
          if (field.Type == TType.Stop)
          {
            break;
          }

          switch (field.ID)
          {
            case 1:
              if (field.Type == TType.I64)
              {
                StartRowOffset = await iprot.ReadI64Async(cancellationToken);
                isset_startRowOffset = true;
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 2:
              if (field.Type == TType.List)
              {
                {
                  var _list210 = await iprot.ReadListBeginAsync(cancellationToken);
                  Rows = new List<global::Apache.Hive.Service.Rpc.Thrift.TRow>(_list210.Count);
                  for(int _i211 = 0; _i211 < _list210.Count; ++_i211)
                  {
                    global::Apache.Hive.Service.Rpc.Thrift.TRow _elem212;
                    _elem212 = new global::Apache.Hive.Service.Rpc.Thrift.TRow();
                    await _elem212.ReadAsync(iprot, cancellationToken);
                    Rows.Add(_elem212);
                  }
                  await iprot.ReadListEndAsync(cancellationToken);
                }
                isset_rows = true;
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 3:
              if (field.Type == TType.List)
              {
                {
                  var _list213 = await iprot.ReadListBeginAsync(cancellationToken);
                  Columns = new List<global::Apache.Hive.Service.Rpc.Thrift.TColumn>(_list213.Count);
                  for(int _i214 = 0; _i214 < _list213.Count; ++_i214)
                  {
                    global::Apache.Hive.Service.Rpc.Thrift.TColumn _elem215;
                    _elem215 = new global::Apache.Hive.Service.Rpc.Thrift.TColumn();
                    await _elem215.ReadAsync(iprot, cancellationToken);
                    Columns.Add(_elem215);
                  }
                  await iprot.ReadListEndAsync(cancellationToken);
                }
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 4:
              if (field.Type == TType.String)
              {
                BinaryColumns = await iprot.ReadBinaryAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 5:
              if (field.Type == TType.I32)
              {
                ColumnCount = await iprot.ReadI32Async(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 1281:
              if (field.Type == TType.List)
              {
                {
                  var _list216 = await iprot.ReadListBeginAsync(cancellationToken);
                  ArrowBatches = new List<global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowBatch>(_list216.Count);
                  for(int _i217 = 0; _i217 < _list216.Count; ++_i217)
                  {
                    global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowBatch _elem218;
                    _elem218 = new global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowBatch();
                    await _elem218.ReadAsync(iprot, cancellationToken);
                    ArrowBatches.Add(_elem218);
                  }
                  await iprot.ReadListEndAsync(cancellationToken);
                }
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 1282:
              if (field.Type == TType.List)
              {
                {
                  var _list219 = await iprot.ReadListBeginAsync(cancellationToken);
                  ResultLinks = new List<global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowResultLink>(_list219.Count);
                  for(int _i220 = 0; _i220 < _list219.Count; ++_i220)
                  {
                    global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowResultLink _elem221;
                    _elem221 = new global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowResultLink();
                    await _elem221.ReadAsync(iprot, cancellationToken);
                    ResultLinks.Add(_elem221);
                  }
                  await iprot.ReadListEndAsync(cancellationToken);
                }
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            default:
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              break;
          }

          await iprot.ReadFieldEndAsync(cancellationToken);
        }

        await iprot.ReadStructEndAsync(cancellationToken);
        if (!isset_startRowOffset)
        {
          throw new TProtocolException(TProtocolException.INVALID_DATA);
        }
        if (!isset_rows)
        {
          throw new TProtocolException(TProtocolException.INVALID_DATA);
        }
      }
      finally
      {
        iprot.DecrementRecursionDepth();
      }
    }

    public async global::System.Threading.Tasks.Task WriteAsync(TProtocol oprot, CancellationToken cancellationToken)
    {
      oprot.IncrementRecursionDepth();
      try
      {
        var tmp222 = new TStruct("TRowSet");
        await oprot.WriteStructBeginAsync(tmp222, cancellationToken);
        var tmp223 = new TField();
        tmp223.Name = "startRowOffset";
        tmp223.Type = TType.I64;
        tmp223.ID = 1;
        await oprot.WriteFieldBeginAsync(tmp223, cancellationToken);
        await oprot.WriteI64Async(StartRowOffset, cancellationToken);
        await oprot.WriteFieldEndAsync(cancellationToken);
        if ((Rows != null))
        {
          tmp223.Name = "rows";
          tmp223.Type = TType.List;
          tmp223.ID = 2;
          await oprot.WriteFieldBeginAsync(tmp223, cancellationToken);
          await oprot.WriteListBeginAsync(new TList(TType.Struct, Rows.Count), cancellationToken);
          foreach (global::Apache.Hive.Service.Rpc.Thrift.TRow _iter224 in Rows)
          {
            await _iter224.WriteAsync(oprot, cancellationToken);
          }
          await oprot.WriteListEndAsync(cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if ((Columns != null) && __isset.columns)
        {
          tmp223.Name = "columns";
          tmp223.Type = TType.List;
          tmp223.ID = 3;
          await oprot.WriteFieldBeginAsync(tmp223, cancellationToken);
          await oprot.WriteListBeginAsync(new TList(TType.Struct, Columns.Count), cancellationToken);
          foreach (global::Apache.Hive.Service.Rpc.Thrift.TColumn _iter225 in Columns)
          {
            await _iter225.WriteAsync(oprot, cancellationToken);
          }
          await oprot.WriteListEndAsync(cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if ((BinaryColumns != null) && __isset.binaryColumns)
        {
          tmp223.Name = "binaryColumns";
          tmp223.Type = TType.String;
          tmp223.ID = 4;
          await oprot.WriteFieldBeginAsync(tmp223, cancellationToken);
          await oprot.WriteBinaryAsync(BinaryColumns, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if (__isset.columnCount)
        {
          tmp223.Name = "columnCount";
          tmp223.Type = TType.I32;
          tmp223.ID = 5;
          await oprot.WriteFieldBeginAsync(tmp223, cancellationToken);
          await oprot.WriteI32Async(ColumnCount, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if ((ArrowBatches != null) && __isset.arrowBatches)
        {
          tmp223.Name = "arrowBatches";
          tmp223.Type = TType.List;
          tmp223.ID = 1281;
          await oprot.WriteFieldBeginAsync(tmp223, cancellationToken);
          await oprot.WriteListBeginAsync(new TList(TType.Struct, ArrowBatches.Count), cancellationToken);
          foreach (global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowBatch _iter226 in ArrowBatches)
          {
            await _iter226.WriteAsync(oprot, cancellationToken);
          }
          await oprot.WriteListEndAsync(cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if ((ResultLinks != null) && __isset.resultLinks)
        {
          tmp223.Name = "resultLinks";
          tmp223.Type = TType.List;
          tmp223.ID = 1282;
          await oprot.WriteFieldBeginAsync(tmp223, cancellationToken);
          await oprot.WriteListBeginAsync(new TList(TType.Struct, ResultLinks.Count), cancellationToken);
          foreach (global::Apache.Hive.Service.Rpc.Thrift.TSparkArrowResultLink _iter227 in ResultLinks)
          {
            await _iter227.WriteAsync(oprot, cancellationToken);
          }
          await oprot.WriteListEndAsync(cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        await oprot.WriteFieldStopAsync(cancellationToken);
        await oprot.WriteStructEndAsync(cancellationToken);
      }
      finally
      {
        oprot.DecrementRecursionDepth();
      }
    }

    public override bool Equals(object that)
    {
      if (!(that is TRowSet other)) return false;
      if (ReferenceEquals(this, other)) return true;
      return global::System.Object.Equals(StartRowOffset, other.StartRowOffset)
        && TCollections.Equals(Rows, other.Rows)
        && ((__isset.columns == other.__isset.columns) && ((!__isset.columns) || (TCollections.Equals(Columns, other.Columns))))
        && ((__isset.binaryColumns == other.__isset.binaryColumns) && ((!__isset.binaryColumns) || (TCollections.Equals(BinaryColumns, other.BinaryColumns))))
        && ((__isset.columnCount == other.__isset.columnCount) && ((!__isset.columnCount) || (global::System.Object.Equals(ColumnCount, other.ColumnCount))))
        && ((__isset.arrowBatches == other.__isset.arrowBatches) && ((!__isset.arrowBatches) || (TCollections.Equals(ArrowBatches, other.ArrowBatches))))
        && ((__isset.resultLinks == other.__isset.resultLinks) && ((!__isset.resultLinks) || (TCollections.Equals(ResultLinks, other.ResultLinks))));
    }

    public override int GetHashCode() {
      int hashcode = 157;
      unchecked {
        hashcode = (hashcode * 397) + StartRowOffset.GetHashCode();
        if ((Rows != null))
        {
          hashcode = (hashcode * 397) + TCollections.GetHashCode(Rows);
        }
        if ((Columns != null) && __isset.columns)
        {
          hashcode = (hashcode * 397) + TCollections.GetHashCode(Columns);
        }
        if ((BinaryColumns != null) && __isset.binaryColumns)
        {
          hashcode = (hashcode * 397) + BinaryColumns.GetHashCode();
        }
        if (__isset.columnCount)
        {
          hashcode = (hashcode * 397) + ColumnCount.GetHashCode();
        }
        if ((ArrowBatches != null) && __isset.arrowBatches)
        {
          hashcode = (hashcode * 397) + TCollections.GetHashCode(ArrowBatches);
        }
        if ((ResultLinks != null) && __isset.resultLinks)
        {
          hashcode = (hashcode * 397) + TCollections.GetHashCode(ResultLinks);
        }
      }
      return hashcode;
    }

    public override string ToString()
    {
      var tmp228 = new StringBuilder("TRowSet(");
      tmp228.Append(", StartRowOffset: ");
      StartRowOffset.ToString(tmp228);
      if ((Rows != null))
      {
        tmp228.Append(", Rows: ");
        Rows.ToString(tmp228);
      }
      if ((Columns != null) && __isset.columns)
      {
        tmp228.Append(", Columns: ");
        Columns.ToString(tmp228);
      }
      if ((BinaryColumns != null) && __isset.binaryColumns)
      {
        tmp228.Append(", BinaryColumns: ");
        BinaryColumns.ToString(tmp228);
      }
      if (__isset.columnCount)
      {
        tmp228.Append(", ColumnCount: ");
        ColumnCount.ToString(tmp228);
      }
      if ((ArrowBatches != null) && __isset.arrowBatches)
      {
        tmp228.Append(", ArrowBatches: ");
        ArrowBatches.ToString(tmp228);
      }
      if ((ResultLinks != null) && __isset.resultLinks)
      {
        tmp228.Append(", ResultLinks: ");
        ResultLinks.ToString(tmp228);
      }
      tmp228.Append(')');
      return tmp228.ToString();
    }
  }

}
