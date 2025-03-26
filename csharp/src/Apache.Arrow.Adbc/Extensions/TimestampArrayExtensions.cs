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
using System.IO;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Extensions
{
    public static class TimestampArrayExtensions
    {
        public static DateTime? GetDateTime(this TimestampArray array, int index)
        {
            var type = (TimestampType)array.Data.DataType;
            DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            long? value = array.GetValue(index);

            if (!value.HasValue)
                return null;

            long ticks;

            switch (type.Unit)
            {
                case TimeUnit.Nanosecond:
                    ticks = value.Value / 100;
                    break;
                case TimeUnit.Microsecond:
                    ticks = value.Value * 10;
                    break;
                case TimeUnit.Millisecond:
                    ticks = value.Value * TimeSpan.TicksPerMillisecond;
                    break;
                case TimeUnit.Second:
                    ticks = value.Value * TimeSpan.TicksPerSecond;
                    break;
                default:
                    throw new InvalidDataException(
                        $"Unsupported timestamp unit <{type.Unit}>");
            }

            return unixEpoch.AddTicks(ticks);
        }
    }
}
