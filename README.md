# dynamic-datalog
Example queries and data for dynamic Datalog computation

---

This repository maintains a list of dynamic Datalog engines, those that update query outputs in response to changes in the input fact sets.

The repository also maintains several paired Datalog queries and input data, meant to exercise dynamic Datalog execution engines in non-trivial ways. These can be found in the [`./problems/`](https://github.com/frankmcsherry/dynamic-datalog/tree/master/problems) subdirectory. These problems may also be independently interesting for evaluating non-dynamic Datalog engines.

### Engines

* [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow) - A Rust framework for incremental data-parallel computation.
* [Declarative Dataflow](https://github.com/comnik/declarative-dataflow) - An interpreted layer on top of Differential Dataflow.
* [Differential Datalog](https://github.com/ryzhyk/differential-datalog) - A compiler targeting Differential Dataflow.
* [IncA](https://github.com/szabta89/IncA) - A framework for incremental program analysis with a Datalog-like language.

We also use the excellent [Souffle](https://souffle-lang.github.io) as a non-dynamic baseline.

### The problems

* [crdt](https://github.com/frankmcsherry/dynamic-datalog/tree/master/problems/crdt) - A [CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) implementation of a shared text editor, from [Martin Kleppmann](https://speakerdeck.com/ept/data-structures-as-queries-expressing-crdts-using-datalog?slide=22).
* [doop](https://github.com/frankmcsherry/dynamic-datalog/tree/master/problems/doop) - A fragment of the [Doop](https://people.cs.umass.edu/~yannis/doop-datalog2.0.pdf) program analysis tool, from [Yannis Smaragdakis](https://yanniss.github.io).
* [galen](https://github.com/frankmcsherry/dynamic-datalog/tree/master/problems/galen) - An inference task in the [GALEN](http://www.openclinical.org/prj_galen.html) medical ontology, from [John Liagouris](http://www.vldb.org/pvldb/vol7/p1993-liagouris.pdf)

### The implementations

For each problem we record reported times for various systems in various configurations, both to perform any query-specific compilation and then execution. These measurements are meant to be representative rather than definitive. All of the systems support multiple worker threads, and could be run in a variety of configurations on a variety of hardware platforms.

Unlike other measurements, the Differential Dataflow measurements are for hand-written code in a larger language, and can reflect implementation and optimizations not easily available within Datalog.

Souffle can often benefit from join planning help; without this help it can take orders of magnitude longer than it could. Such help is currently only provided for the Doop benchmark, and measurements for other queries could improve in the future.

#### The CRDT benchmark

| Engine                | Compilation   | Evaluation    | Cores | Notes     |
|----------------------:|--------------:|--------------:|------:|----------:|
| Soufflé (interpreted) | 0s            | 1000s+ (DNF)  |     1 | Laptop    |
| Soufflé (compiled)    | 10.15s        | 1000s+ (DNF)  |     1 | Laptop    |
| Differential Dataflow | 166.26s       | 3.44s         |     1 | Laptop    |
| Declarative Dataflow  | 0s            |               |       |           |
| Differential Datalog  |               |               |       |           |
| IncA                  |               |               |       |           |

The CRDT benchmark contains stratified negation, as well as several "maximization" idioms represented in Datalog using a quadratic number of facts, which can be more efficiently implemented as data-parallel `reduce` operations.

#### The DOOP benchmark

| Engine                | Compilation   | Evaluation    | Cores | Notes     |
|----------------------:|--------------:|--------------:|------:|----------:|
| Soufflé (interpreted) | 0s            | 762.14s       |     1 | Laptop    |
| Soufflé (compiled)    | 93.43s        | 111.76s       |     1 | Laptop    |
| Differential Dataflow | 237.43s       | 161.58s       |     1 | Laptop    |
| Declarative Dataflow  | 0s            |               |       |           |
| Differential Datalog  |               |               |       |           |
| IncA                  |               |               |       |           |

The DOOP benchmark is just really quite large.

#### The GALEN benchmark

| Engine                | Compilation   | Evaluation    | Cores | Notes     |
|----------------------:|--------------:|--------------:|------:|----------:|
| Soufflé (interpreted) | 0s            | 1000s+ (DNF)  |     1 | Laptop    |
| Soufflé (compiled)    | 9.31s         | 198.19s       |     1 | Laptop    |
| Differential Dataflow |               |               |     1 | Laptop    |
| Declarative Dataflow  | 0s            |               |       |           |
| Differential Datalog  |               |               |       |           |
| IncA                  |               |               |       |           |

The GALEN bencmark contains joins on highly skewed keys, for which correct or adaptive join orders are important.
