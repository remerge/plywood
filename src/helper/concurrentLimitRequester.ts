/*
 * Copyright 2012-2015 Metamarkets Group Inc.
 * Copyright 2015-2020 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { DatabaseRequest, PlywoodRequester } from 'plywood-base-api';
import { PassThrough } from 'readable-stream';
import { pipeWithError } from './utils';
import { Gauge } from 'prom-client';

function generateRequestId() {
  return Math.random().toString(26).slice(2);
}

export interface ConcurrentLimitRequesterParameters<T> {
  requester: PlywoodRequester<T>;
  concurrentLimit: int;
  concurrentRequests: Gauge;
}

interface QueueItem<T> {
  request: DatabaseRequest<T>;
  stream: PassThrough;
}

export function concurrentLimitRequesterFactory<T>(parameters: ConcurrentLimitRequesterParameters<T>): PlywoodRequester<T> {
  let requester = parameters.requester;
  let concurrentLimit = parameters.concurrentLimit || 5;
  let concurrentRequests = parameters.concurrentRequests;

  if (typeof concurrentLimit !== "number") throw new TypeError("concurrentLimit should be a number");

  let requestQueue: QueueItem<T>[] = [];
  let outstandingRequests: int = 0;
  let runningRequestIds: string[] = []

  setInterval(() => {
    console.log(`Concurrent Limit: ${outstandingRequests} / ${concurrentLimit}. Queue length: ${requestQueue.length}, Running Requests: ${runningRequestIds.join(', ')}`)
  }, 60 * 1000)

  function requestFinished(): void {
    outstandingRequests--;
    concurrentRequests.dec();
    if (!(requestQueue.length && outstandingRequests < concurrentLimit)) return;
    let queueItem = requestQueue.shift();
    outstandingRequests++;
    concurrentRequests.inc();
    const requestId = generateRequestId();
    runningRequestIds.push(requestId);
    console.log(`Starting request ${requestId} (${outstandingRequests}/${concurrentLimit}): ${(queueItem.request.query as any).queryType} from queue`);

    const stream = requester(queueItem.request);

    const requestFinishedOnce = getOnceCallback(() => {
      runningRequestIds = runningRequestIds.filter((id) => id !== requestId)
      console.log(`Request finished ${requestId} from Queue`);
      requestFinished();
    });
    stream.on('error', () => requestFinishedOnce());
    stream.on('end', () => requestFinishedOnce());

    queueItem.stream.on('error', (error) => {
      requestFinishedOnce(() => {
        console.log(`Error on PassThrough for request ${requestId}`)
      })
    });

    pipeWithError(stream, queueItem.stream);
  }

  return (request: DatabaseRequest<T>) => {
    if (outstandingRequests < concurrentLimit) {
      outstandingRequests++;
      concurrentRequests.inc();
      const requestId = generateRequestId();
      runningRequestIds.push(requestId);
      console.log(`Starting request ${requestId} (${outstandingRequests}/${concurrentLimit}): ${(request.query as any).queryType}`);
      const stream = requester(request);

      const requestFinishedOnce = getOnceCallback(() => {
        console.log(`Request finished ${requestId}`);
        runningRequestIds = runningRequestIds.filter((id) => id !== requestId)
        requestFinished();
      });
      stream.on('error', () => requestFinishedOnce());
      stream.on('end', () => requestFinishedOnce());

      return stream;
    } else {
      const stream = new PassThrough({ objectMode: true });
      requestQueue.push({
        request,
        stream
      });
      return stream;
    }
  };
}

function getOnceCallback(callback: () => void) {
  let called = false;

  return (optionalCallback = () => {}) => {
    if (!called) {
      called = true;
      callback();
      optionalCallback();
    }
  };
}
