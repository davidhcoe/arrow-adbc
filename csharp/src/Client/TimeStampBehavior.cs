using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Controls the behavior of how TimeStampArrays should be handled in the results.
    /// </summary>
    public enum TimeStampBehavior
    {
        DateTimeOffset,
        DateTime
    }
}
