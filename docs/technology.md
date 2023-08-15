# Technical Details

## `asyncio`

Nodestream is built on top of the more modern aspects of python. Primarily asyncio to improve performance. 
To understand how it does so, it’s important to understand what exactly asnycio is and how it works. 

`asyncio` is a way of modeling concurrency in an application. 
Note that concurrency is different than parallelism. 
Parallelism deals with multiple operations executing simultaneously while concurrency deals with tasks that can be executed independently of each other and start, run, and complete in overlapping time periods


![concurrency](https://files.realpython.com/media/Screen_Shot_2018-10-17_at_3.18.44_PM.c02792872031.jpg)

!!! note

    Images from [https://realpython.com/async-io-python/](https://realpython.com/async-io-python/)

`asyncio` operates on the observation that in some systems, the cpu is spending a lot of time _blocking_ or waiting for IO operations such as network calls, file reads, and the like to finish before it can continue operating. 
To make better use of the CPU, `asyncio` introduces a series of language features that facilitate defining tasks and signaling when they need to wait. The language runtime can the “swap in” a different task that it can work on that’s not (currently) IO bound. 
In workloads where there is an abundant amout of IO, the overhead of managing these tasks is worth it because  we are able to make better use CPU time that would otherwise be wasted.

![asyncio](https://eng.paxos.com/hs-fs/hubfs/_02_Paxos_Engineering/Event-Loop.png?width=800&name=Event-Loop.png)

!!! note

    Images from [https://eng.paxos.com/python-3s-killer-feature-asyncio](https://eng.paxos.com/python-3s-killer-feature-asyncio)

So how does nodestream leverage it? All the performance sensitive apis in nodestream allow for asynchronous operations. Most importantly, each step in a pipeline is executed asyncronously. To do so, each step is given two components at exectuion time:

* A reference to the step before it
* An outbox which is used to store completed output

Each step runs in its own asynchronous loop awaiting results from the upstream step, processing them, and putting results in the outbox.
This design allows for each step to execute independently and not be constrained by IO bottlenecks down stream. 
Take for example a standard pipeline of three components:

1. An file extractor
1. An interpreter
1. A Graph database writer

The slowest step in the pipeline is the graph database writer. 
It takes large chunks of data, converts it all to queries and submits generally long running queries it has to await.
While this is happening, python can suspend the task waiting on the database allowing the other steps to continue processing.
This allows for the extractor and interpreter to continue processing data and not be constrained by the database writer.
