/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections;
using System.Data;
using System.Data.Common;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// An ADBC implementation of <see cref="DbParameter"/>.
    /// </summary>
    public class AdbcParameter : DbParameter
    {
        public AdbcParameter()
        {
            
        }

        public AdbcParameter(string name, object value)
        {
            this.ParameterName = name;
            this.Value = value;
        }

        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get => ParameterDirection.Input; set => throw new NotImplementedException(); }
        public override bool IsNullable { get; set; }
        public override string ParameterName { get; set; }
        public override int Size { get; set; }
        public override string SourceColumn { get; set; }
        public override bool SourceColumnNullMapping { get; set; }
        public override object Value { get; set ; }

        public override void ResetDbType() => throw new NotImplementedException();
    }

    /// <summary>
    /// An ADBC implementation of <see cref="DbParameterCollection"/>.
    /// </summary>
    public class AdbcParameterCollection : DbParameterCollection
    {
        private ArrayList parameters = new ArrayList();

        public override int Add(object value)
        {
            if (!(value is AdbcParameter))
            {
                throw new ArgumentException("Only AdbcParameter objects can be added to the collection.");
            }

            parameters.Add((AdbcParameter)value);
            return parameters.Count - 1;
        }

        public override void Clear()
        {
            parameters.Clear();
        }

        public override bool Contains(string value)
        {
            foreach (DbParameter parameter in parameters)
            {
                if (String.Equals(parameter.ParameterName, value, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        public override bool Contains(object value)
        {
            return parameters.Contains(value);
        }

        public override int Count => parameters.Count;

        public override object SyncRoot => throw new NotImplementedException();

        public override IEnumerator GetEnumerator()
        {
            return parameters.GetEnumerator();
        }

        protected override DbParameter GetParameter(int index)
        {
            return (DbParameter)parameters[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            foreach (DbParameter parameter in parameters)
            {
                if (String.Equals(parameter.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                {
                    return parameter;
                }
            }

            throw new IndexOutOfRangeException("Parameter not found.");
        }

        public override int IndexOf(object value)
        {
            if (!(value is AdbcParameter))
            {
                throw new ArgumentException("Only AdbcParameter objects can be looked up.");
            }

            return parameters.IndexOf((AdbcParameter)value);
        }

        public override int IndexOf(string parameterName)
        {
            for (int i = 0; i < parameters.Count; i++)
            {
                if (String.Equals(((AdbcParameter)parameters[i]).ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }

            return -1;
        }

        public override void Insert(int index, object value)
        {
            if (!(value is AdbcParameter))
            {
                throw new ArgumentException("Only AdbcParameter objects can be inserted into the collection.");
            }

            parameters.Insert(index, (AdbcParameter)value);
        }

        public override void Remove(object value)
        {
            if (!(value is AdbcParameter))
            {
                throw new ArgumentException("Only AdbcParameter objects can be removed from the collection.");
            }

            parameters.Remove((AdbcParameter)value);
        }

        public override void RemoveAt(int index)
        {
            parameters.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index != -1)
            {
                RemoveAt(index);
            }
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            if (!(value is AdbcParameter))
            {
                throw new ArgumentException("Only AdbcParameter objects can be set.");
            }

            parameters[index] = (AdbcParameter) value;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            int index = IndexOf(parameterName);
            if (index != -1)
            {
                SetParameter(index, value);
            }
        }

        public override void AddRange(System.Array values)
        {
            foreach(var value in values)
            {
                Add(value);
            }
        }

        public override void CopyTo(System.Array array, int index)
        {
            parameters.CopyTo(array, index);
        }
    }
}
