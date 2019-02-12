// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.WebJobs.Script.Diagnostics;

namespace Microsoft.Azure.WebJobs.Script.Rpc
{
    public class LanguageWorkerChannelMetadata
    {
        public LanguageWorkerChannelMetadata(string rootScriptPath, IMetricsLogger metricsLogger)
        {
            RootScriptPath = rootScriptPath;
            MetricsLogger = metricsLogger;
        }

        internal string RootScriptPath { get; set; }

        internal IMetricsLogger MetricsLogger { get; set; }
    }
}
