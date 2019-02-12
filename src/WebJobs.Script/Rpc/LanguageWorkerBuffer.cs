// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.WebJobs.Script.Description;
using Microsoft.Azure.WebJobs.Script.Eventing;
using Microsoft.Azure.WebJobs.Script.Eventing.Rpc;
using Microsoft.Azure.WebJobs.Script.Grpc.Messages;
using MsgType = Microsoft.Azure.WebJobs.Script.Grpc.Messages.StreamingMessage.ContentOneofCase;

namespace Microsoft.Azure.WebJobs.Script.Rpc
{
    public class LanguageWorkerBuffer
    {
        private readonly IScriptEventManager _eventManager;

        private IDictionary<string, BufferBlock<ScriptInvocationContext>> _functionInputBuffers = new Dictionary<string, BufferBlock<ScriptInvocationContext>>();

        private IObservable<FunctionRegistrationContext> _functionRegistrations;

        // List to track the different event subscriptions we have
        private List<IDisposable> _eventSubscriptions = new List<IDisposable>();

        private List<IDisposable> _inputLinks = new List<IDisposable>();
        private bool _disposed;
        private int counter;

        // probably some way of keeping track of the invocations
        private List<LanguageWorkerChannel> _channels = new List<LanguageWorkerChannel>();

        public LanguageWorkerBuffer(IScriptEventManager eventManager, IObservable<FunctionRegistrationContext> functionRegistrations, int numberWorkers)
        {
            _disposed = false;
            counter = 0;
            _eventManager = eventManager;
            MaxNumberWorkers = numberWorkers;
            //_loadBalancerEvents = _eventManager.OfType<LoadBalancerEvent>();

            _functionRegistrations = functionRegistrations;

            _eventSubscriptions.Add(_functionRegistrations.Subscribe(BufferSetup));
        }

        //keep track of the different language worker channels that we have:
        internal int MaxNumberWorkers { get; set; }

        internal int CurrentNumberWorkers { get; set; }

        public void BufferSetup(FunctionRegistrationContext context)
        {
            FunctionMetadata metadata = context.Metadata;

            // associate the invocation input buffer with the function
            _functionInputBuffers[context.Metadata.FunctionId] = context.InputBuffer;

            var inputBuffer = _functionInputBuffers[context.Metadata.FunctionId];
            // link the invocation inputs to the invoke call
            var invokeBlock = new ActionBlock<ScriptInvocationContext>(ctx => FunctionInvocation(ctx));
            var disposableLink = inputBuffer.LinkTo(invokeBlock);
            _inputLinks.Add(disposableLink);
        }

        private void FunctionInvocation(ScriptInvocationContext context)
        {
            // Chose which language worker channel to send it to then emmit an event
            var c = context;
            var ca = _channels[counter % CurrentNumberWorkers];
            counter++;
            //Channel.SendInvocationRequest(c);
            ca.SendInvocationRequest(c);
        }

        // just copything the dispose method from the language worker channel - we can modify as needed
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // unlink function inputs
                    foreach (var link in _inputLinks)
                    {
                        link.Dispose();
                    }
                }
            }
        }

        public void AddChannel(ILanguageWorkerChannel channel)
        {
            _channels.Add(channel as LanguageWorkerChannel);
            CurrentNumberWorkers++;
        }
    }
}
