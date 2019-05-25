# dynamic-datalog
Example queries and data for dynamic Datalog computation

---

This repository maintains several paired Datalog queries and input data, meant to exercise dynamic Datalog execution engines.
These queries have been chosen to be non-trivial, each exercising the engines in different ways.

### The problems

The `doop` query and data come from Yannis Smaragdakis. The query is a simplification of [the Doop program analysis package](https://people.cs.umass.edu/~yannis/doop-datalog2.0.pdf) for the analysis of Java programs, and the data were extracted from a small Java program.

The `galen` query comes from John Liagouris, and the data from [the GALEN project](http://www.openclinical.org/prj_galen.html). The query comes from the paper [Efficient Identification of Implicit Facts in Incomplete OWL2-EL Knowledge Bases∗](http://www.vldb.org/pvldb/vol7/p1993-liagouris.pdf), and describes an inferencing task in medical ontologies.

The `crdt` query and data come from Martin Kleppmann. The query describes update rules for a [conflict-free replicated data type](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) implementation of a shared text editor, described in a Dagstuhl talk [Data structures as queries: Expressing CRDTs using Datalog](https://speakerdeck.com/ept/data-structures-as-queries-expressing-crdts-using-datalog?slide=22). The data are a sequence of edits Martin made while typing some associated text.

### The implementations

This is where we'll list implementations, reported behavior, and links to repositories!

