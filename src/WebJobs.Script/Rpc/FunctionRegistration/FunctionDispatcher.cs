// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.WebJobs.Script.Description;
using Microsoft.Azure.WebJobs.Script.Diagnostics;
using Microsoft.Azure.WebJobs.Script.Eventing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using FunctionMetadata = Microsoft.Azure.WebJobs.Script.Description.FunctionMetadata;

namespace Microsoft.Azure.WebJobs.Script.Rpc
{
    internal class FunctionDispatcher : IFunctionDispatcher
    {
        private readonly ILoggerFactory _loggerFactory;
        private IScriptEventManager _eventManager;
        private IEnumerable<WorkerConfig> _workerConfigs;
        private CreateChannel _channelFactory;
        private ILanguageWorkerChannelManager _languageWorkerChannelManager;
        private ConcurrentDictionary<string, LanguageWorkerState> _workerStates = new ConcurrentDictionary<string, LanguageWorkerState>();
        private LanguageWorkerErrorHandler _errorHandler;
        private bool disposedValue = false;

        private LanguageWorkerChannelMetadata _channelMetadata;

        public FunctionDispatcher(IOptions<ScriptJobHostOptions> scriptHostOptions,
            IMetricsLogger metricsLogger,
            IScriptEventManager eventManager,
            ILoggerFactory loggerFactory,
            IOptions<LanguageWorkerOptions> languageWorkerOptions,
            ILanguageWorkerChannelManager languageWorkerChannelManager)
        {
            _languageWorkerChannelManager = languageWorkerChannelManager;
            _eventManager = eventManager;
            _loggerFactory = loggerFactory;
            _workerConfigs = languageWorkerOptions.Value.WorkerConfigs;

            // create the new error handler
            _channelMetadata = new LanguageWorkerChannelMetadata(scriptHostOptions.Value.RootScriptPath, metricsLogger);
            _errorHandler = new LanguageWorkerErrorHandler(_eventManager, _languageWorkerChannelManager, _channelMetadata);
        }

        public IDictionary<string, LanguageWorkerState> LanguageWorkerChannelStates => _workerStates;

        public LanguageWorkerErrorHandler ErrorHandler => _errorHandler;

        internal CreateChannel ChannelFactory
        {
            get
            {
                if (_channelFactory == null)
                {
                    _channelFactory = (language, registrations, attemptCount) =>
                    {
                        var languageWorkerChannel = _languageWorkerChannelManager.CreateLanguageWorkerChannel(Guid.NewGuid().ToString(), _channelMetadata.RootScriptPath, language, registrations, _channelMetadata.MetricsLogger, attemptCount);
                        languageWorkerChannel.StartWorkerProcess();
                        return languageWorkerChannel;
                    };
                }
                return _channelFactory;
            }
        }

        public bool IsSupported(FunctionMetadata functionMetadata, string workerRuntime)
        {
            if (string.IsNullOrEmpty(functionMetadata.Language))
            {
                return false;
            }
            if (string.IsNullOrEmpty(workerRuntime))
            {
                return true;
            }
            return functionMetadata.Language.Equals(workerRuntime, StringComparison.OrdinalIgnoreCase);
        }

        public void Initialize(string workerRuntime, IEnumerable<FunctionMetadata> functions)
        {
            _languageWorkerChannelManager.ShutdownStandbyChannels(functions);
            workerRuntime = workerRuntime ?? Utility.GetWorkerRuntime(functions);

            if (Utility.IsSupportedRuntime(workerRuntime, _workerConfigs))
            {
                ILanguageWorkerChannel initializedChannel = _languageWorkerChannelManager.GetChannel(workerRuntime);
                if (initializedChannel != null)
                {
                    // TODO Part 2 - this mainly happens with java rather than node. In node we use the second part
                    LanguageWorkerState state = CreateWorkerState(workerRuntime);
                    LanguageWorkerBuffer buffer = _languageWorkerChannelManager.CreateLanguageWorkerBuffer(state.Functions);
                    // need to add to the buffer the existing initialized channel !!!
                }
                else
                {
                    // We can create the different language workers ..
                    // CHECK - there isn't a race condition b/w creating the channel and the state --> I know how to fix this by seperating out what happens in
                    // the create worker state
                    LanguageWorkerState state = CreateWorkerState(workerRuntime);
                    _workerStates[workerRuntime] = state;
                    LanguageWorkerBuffer buffer = _languageWorkerChannelManager.CreateLanguageWorkerBuffer(state.Functions);
                }
            }
        }

        private LanguageWorkerState CreateWorkerState(string runtime)
        {
            var state = new LanguageWorkerState();
            WorkerConfig config = _workerConfigs.Where(c => c.Language.Equals(runtime, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            _workerStates[runtime] = state;
            return state;
        }

        public void Register(FunctionRegistrationContext context)
        {
            _workerStates[context.Metadata.Language].Functions.OnNext(context);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    //_workerErrorSubscription.Dispose();
                    //foreach (var subscription in _workerStateSubscriptions)
                    //{
                    //    subscription.Dispose();
                    //}
                    foreach (var pair in _workerStates)
                    {
                        pair.Value.Functions.Dispose();
                    }
                    // TODO #3296 - send WorkerTerminate message to shut down language worker process gracefully (instead of just a killing)
                    // TODO : need the channel manager to shutdown all of the different channels that exist
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
    }
}
