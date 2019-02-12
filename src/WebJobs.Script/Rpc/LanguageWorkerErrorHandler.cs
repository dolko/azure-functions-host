// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.WebJobs.Script.Description;
using Microsoft.Azure.WebJobs.Script.Diagnostics;
using Microsoft.Azure.WebJobs.Script.Eventing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Microsoft.Azure.WebJobs.Script.Rpc
{
    public class LanguageWorkerErrorHandler
    {
        private IDisposable _workerErrorSubscription;
        private IScriptEventManager _eventManager;
        private ILanguageWorkerChannelManager _languageWorkerChannelManager;
        private int _count;
        private IList<IDisposable> _workerStateSubscriptions = new List<IDisposable>();
        private LanguageWorkerChannelMetadata _channelMetadata;

        public LanguageWorkerErrorHandler(IScriptEventManager eventManager, ILanguageWorkerChannelManager languageWorkerChannelManager, LanguageWorkerChannelMetadata channelMetadata)
        {
            _languageWorkerChannelManager = languageWorkerChannelManager;
            _eventManager = eventManager;
            _channelMetadata = channelMetadata;
            _count = 0;
            _workerErrorSubscription = _eventManager.OfType<WorkerErrorEvent>()
               .Subscribe(WorkerError);
        }

        internal List<Exception> Errors { get; set; } = new List<Exception>();

        public void WorkerError(WorkerErrorEvent workerError)
        {
            Errors.Add(workerError.Exception);
            bool isPreInitializedChannel = _languageWorkerChannelManager.ShutdownChannelIfExists(workerError.WorkerId);
            // need to fix up the disposing here
            if (!isPreInitializedChannel)
            {
                // need to do some sort of disposal here based on the manager
            }
            RestartWorkerChannel(workerError.Language, workerError.WorkerId);
            _count++;
        }

        private void RestartWorkerChannel(string language, string workerId)
        {
            if (_count < 3)
            {
                var languageWorkerChannel = _languageWorkerChannelManager.CreateLanguageWorkerChannel(Guid.NewGuid().ToString(), _channelMetadata.RootScriptPath, language, registrations, _channelMetadata.MetricsLogger, _count);
                languageWorkerChannel.StartWorkerProcess();
            }
            else
            {
                PublishWorkerProcessErrorEvent(language, workerId);
            }
        }

        private void PublishWorkerProcessErrorEvent(string language, string workerId)
        {
            var exMessage = $"Failed to start language worker for: {language}";
            var languageWorkerChannelException = (Errors != null && Errors.Count > 0) ? new LanguageWorkerChannelException(exMessage, new AggregateException(Errors.ToList())) : new LanguageWorkerChannelException(exMessage);
            var errorBlock = new ActionBlock<ScriptInvocationContext>(ctx =>
            {
                ctx.ResultSource.TrySetException(languageWorkerChannelException);
            });
            _workerStateSubscriptions.Add(erroredWorkerState.Functions.Subscribe(reg =>
            {
                erroredWorkerState.AddRegistration(reg);
                reg.InputBuffer.LinkTo(errorBlock);
            }));
            _eventManager.Publish(new WorkerProcessErrorEvent(workerId, language, languageWorkerChannelException));
        }
    }
}
