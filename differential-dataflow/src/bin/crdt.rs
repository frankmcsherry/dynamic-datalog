extern crate timely;
extern crate differential_dataflow;

use std::io::{BufRead, BufReader};
use std::fs::File;

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;
use differential_dataflow::Collection;
use timely::dataflow::ProbeHandle;


type Id = (usize, usize);

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();
        let peers = worker.peers();
        let index = worker.index();
        let mut probe = ProbeHandle::new();

        let (mut insert, mut remove, mut assign) = worker.dataflow::<usize,_,_>(move |scope| {

            // input handles for the three input collections.
            let (i_handle, insert) = scope.new_collection::<(Id, Id),isize>();
            let (r_handle, remove) = scope.new_collection::<Id,isize>();
            let (a_handle, assign) = scope.new_collection::<(Id, Id, String),isize>();

            // Convert pairs of identifiers to hashed values.
            use crate::differential_dataflow::Hashable;
            let insert = insert.map(|(source, target)| (source.hashed(), target.hashed()));
            let remove = remove.map(|node| node.hashed());
            let assign = assign.map(|(thing1, thing2, text)| (thing1.hashed(), thing2.hashed(), text));

            let result = rga(insert, remove, assign);

            result.probe_with(&mut probe);
            result.map(|_| ()).consolidate().inspect(|x| println!("result: {:?}", x));

            (i_handle, r_handle, a_handle)
        });

        let mut args = std::env::args().skip(1);
        let path = args.next().expect("missing arg: trace path");

        let filename = format!("{}/insert.txt", path);
        let file = BufReader::new(File::open(filename).unwrap());
        for (count, readline) in file.lines().enumerate() {
            if count % peers == index {
                let line = readline.ok().expect("read error");
                let mut elts = line[..].split_whitespace();
                let id_ctr:   usize = elts.next().unwrap().parse().ok().expect("malformed id_ctr");
                let id_node:  usize = elts.next().unwrap().parse().ok().expect("malformed id_node");
                let ref_ctr:  usize = elts.next().unwrap().parse().ok().expect("malformed ref_ctr");
                let ref_node: usize = elts.next().unwrap().parse().ok().expect("malformed ref_node");
                insert.insert(((id_ctr, id_node), (ref_ctr, ref_node)));
                assign.insert(((id_ctr, id_node), (id_ctr, id_node), "".to_string()));
            }
        }

        let filename = format!("{}/remove.txt", path);
        let file = BufReader::new(File::open(filename).unwrap());
        for (count, readline) in file.lines().enumerate() {
            if count % peers == index {
                let line = readline.ok().expect("read error");
                let mut elts = line[..].split_whitespace();
                let ref_ctr:  usize = elts.next().unwrap().parse().ok().expect("malformed ref_ctr");
                let ref_node: usize = elts.next().unwrap().parse().ok().expect("malformed ref_node");
                remove.insert((ref_ctr, ref_node));
            }
        }

        insert.close();
        remove.close();
        assign.close();
        println!("{:?}", timer.elapsed());

        while worker.step() {
        }
        println!("{:?}\tcomplete", timer.elapsed());

    }).unwrap();
}

use std::hash::Hash;
use timely::dataflow::Scope;
use differential_dataflow::{ExchangeData, lattice::Lattice};

/// Provided a collection representing a directed forest, connect all nodes to their root.
fn collapse_tree1<G, T>(edges: Collection<G, (T, T)>) -> Collection<G, (T, T)>
where
    G: Scope,
    G::Timestamp: Lattice,
    T: ExchangeData+Hash,
{
    // Repeatedly join (a,b) with (b,c), to form (a,c). Whenever we find a match, we replace
    // (a,b) with (a,c). When we stop finding matches (which only happens when b is the root)
    // the replacement stops and the iteration converges (for each a, independently).
    edges
        .iterate(|inner| {
            // We could use mutual recursion to develop two collections: those in progress,
            // and those that are complete. We join the in progress edges with both those
            // in progress and those that are complete; there should be exactly one match,
            // based on whether the target is in progress or complete, and we can land the
            // edge in the corresponding collection.
            //
            // The main benefit is that complete edges do not seek a match for their target,
            // and we do not eventually overwhelm the worker responsible for the tree root.
            let transp = inner.map(|(source,target)| (target,source));
            transp
                .join_map(&inner, |via, source, target| (source.clone(), via.clone(), target.clone()))
                .explode(|(source, via, target)| vec![((source.clone(), target),1), ((source, via),-1)])
                .concat(&inner)
                .consolidate()
        })
        .consolidate()
}

/// Provided a collection representing a directed forest, connect all nodes to their root.
fn collapse_tree2<G, T>(edges: Collection<G, (T, T)>) -> Collection<G, (T, T)>
where
    G: Scope,
    G::Timestamp: Lattice,
    T: ExchangeData+Hash,
{
    edges
        .map(|(source, target)| (source, (target, 1)))
        .iterate(|inner| {
            let transp = inner.map(|(s,(t,l))| (t,(s,l)));
            transp
                .join_map(&inner, |_via, (source, len1), (target, len2)| (source.clone(), (len1+len2, target.clone())))
                .concat(&inner.map(|(source,(target,length))| (source,(length,target))))
                .consolidate()
                .reduce(|_source, input, output| {
                    if let Some(((length, target), _)) = input.last() {
                        output.push(((target.clone(), *length), 1));
                    }
                })
        })
        .map(|(source, (target, _length))| (source, target))
        .consolidate()
}

/// Provided a collection containing multiple directed paths, connect all path endpoints.
///
/// For example, if the collection contains `(a,b), (b,c), ... (y,z)` we would produced `(a,z)`.
/// If there are multiple disjoint paths, the result contain one pair (the endpoints) for each.
/// The pairs will be ordered with the smaller endpoint first, and the caller should take care
/// to return them to their original order.
fn collapse_paths<G, T>(edges: Collection<G, (T, T)>) -> Collection<G, (T, T)>
where
    G: Scope,
    G::Timestamp: Lattice,
    T: ExchangeData+Hash,
{
    // Key the edge by the minimum, to attempt to fuse.
    edges.iterate(|inner| {

        // Pick out candidates in each direction (as source, and as target).
        let sources = inner.filter(|(source, via)| source > via);
        let targets = inner.filter(|(via, target)| via < target);
        // Connect sources and targets, retract when we find matches.
        sources
            .map(|(source, via)| (via, source))
            .join_map(&targets, |via, source, target| (source.clone(), via.clone(), target.clone()))
            .explode(|(source, via, target)| {std::array::IntoIter::new([
                ((source.clone(), target.clone()), 1),
                ((source, via.clone()), -1),
                ((via, target), -1),
            ])})
            .concat(inner)
            .consolidate()

    })
    .consolidate()
}

fn rga<G, T>(
    insert: Collection<G, (T, T)>,
    remove: Collection<G, T>,
    assign: Collection<G, (T, T, String)>,
) -> Collection<G, (T, T)>
where
    G: Scope,
    G::Timestamp: Lattice+timely::order::TotalOrder,
    T: ExchangeData+Hash,
{
    let has_child = insert.map(|(_, x)| x).distinct_total();

    let insert_by_parent = insert.map(|(x,y)| (y,x));

    let siblings =
    insert_by_parent
        .reduce(move |_key, input, output| {
            for i in 1..input.len() {
                output.push(((input[i].0.clone(), input[i-1].0.clone()), 1));
            }
        });

    let next_sibling = siblings.map(|(_parent, pair)| pair);

    let first_child =
    siblings
        .map(|(parent, (_child1, child2))| (parent, child2))
        .negate()
        .concat(&insert_by_parent);

    let last_child =
        siblings
            .map(|(parent, (child1, _child2))| (parent, child1))
            .negate()
            .concat(&insert_by_parent);

    let to_collapse = last_child.map(|(parent, child)| (child, parent));
    let to_collapse = collapse_paths(to_collapse.clone());

    // Start from childen with no next sibling, and climb up their ancestors
    // to the first that does have a next sibling.
    let ancestor_with_next_sibling = collapse_tree1(to_collapse);

    let next_sibling_to_take = next_sibling.semijoin(&has_child);
    let substitutions = ancestor_with_next_sibling
        .antijoin(&has_child)
        .map(|(child, ancestor)| (ancestor, child))
        .join_map(&next_sibling_to_take, |_ancestor, child, sibling| (child.clone(), sibling.clone()));

    // Spell out the linear sequence of insertions:
    // 1. Parents pointing to their first child
    // 2. Children pointing to their next sibling.
    // 3. The last child pointing to the first ancestor next sibling.
    let next_elem = first_child
        .concat(&next_sibling_to_take.negate().concat(&next_sibling))
        .concat(&substitutions);

    let current_value =
    assign
        .map(|(id, elem, value)| (id, (elem, value)))
        .antijoin(&remove)
        .map(|(_, pair)| pair);

    let has_value = current_value.map(|(elem,_)| elem).distinct_total();

    let value_step = next_elem.semijoin(&has_value);            // (from,to) where has_value(from).
    let blank_step = value_step.negate().concat(&next_elem);    // (from,to) where !has_value(from).
    let blank_step = collapse_paths(blank_step);

    // Paths of the form value (blank)^*.
    let value_blank_star = value_step.iterate(|inner| {

        let value_step = value_step.enter(&inner.scope());
        let blank_step = blank_step.enter(&inner.scope());

        inner.map(|(from, via)| (via, from))
             .join_map(&blank_step, |_via, from, to| (from.clone(), to.clone()))
             .concat(&value_step)
             .distinct()
    });

    // Paths of the form value (blank)^* value.
    let next_visible =
    value_blank_star
        .map(|(prev, next)| (next,prev))
        .semijoin(&has_value)
        .map(|(next,prev)| (prev,next));

    next_visible
}