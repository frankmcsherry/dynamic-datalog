extern crate timely;
// extern crate graph_map;
extern crate differential_dataflow;

use std::io::{BufRead, BufReader};
use std::fs::File;

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;
// use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::Collection;
use timely::dataflow::ProbeHandle;

type Id = (usize, usize);

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();
        let peers = worker.peers();
        let index = worker.index();
        let mut probe = ProbeHandle::new();

        let (mut insert, mut remove, mut assign) = worker.dataflow::<usize,_,_>(|scope| {

            // input handles for the three input collections.
            let (i_handle, insert) = scope.new_collection::<(Id, Id),isize>();
            let (r_handle, remove) = scope.new_collection::<Id,isize>();
            let (a_handle, assign) = scope.new_collection::<(Id, Id, String),isize>();

            let has_child = insert.map(|(_, x)| x).distinct_total();

            let insert_by_parent = insert.map(|(x,y)| (y,x));

            let siblings =
            insert_by_parent
                .reduce(move |_key, input, output| {
                    for i in 1..input.len() {
                        output.push(((*input[i].0, *input[i-1].0), 1));
                    }
                });

            let next_sibling = siblings.map(|(_parent, pair)| pair);

            let first_child =
            siblings
                .map(|(parent, (_child1, child2))| (parent, child2))
                .negate()
                .concat(&insert_by_parent);

            let has_next_sibling: Collection<_,Id,_> = next_sibling.map(|(x,_)| x).distinct_total();

            // NB: Done using `iterate` because no mutual recursion.
            let next_sibling_anc = next_sibling.iterate(|inner| {

                let insert = insert.enter(&inner.scope());
                let has_next_sibling = has_next_sibling.enter(&inner.scope());
                let next_sibling = next_sibling.enter(&inner.scope());

                insert.antijoin(&has_next_sibling)
                      .map(|(start, parent)| (parent, start))
                      .join_map(&inner, |_parent, &start, &next| (start, next))
                      .concat(&next_sibling)
                      .distinct()

            });

            // next_sibling_anc.map(|_|()).consolidate().inspect(|x| println!("nsa: {:?}", x.2));

            let next_elem = first_child.concat(&next_sibling_anc.antijoin(&has_child));

            let current_value =
            assign
                .map(|(id, elem, value)| (id, (elem, value)))
                .antijoin(&remove)
                .map(|(_, (elem, value))| (elem, value));

            let has_value = current_value.map(|(elem,_)| elem).distinct_total();

            let value_step = next_elem.semijoin(&has_value);            // (from,to) where has_value(from).
            let blank_step = value_step.negate().concat(&next_elem);    // (from,to) where !has_value(from).

            // Paths of the form value (blank)^*.
            let value_blank_star = value_step.iterate(|inner| {

                let value_step = value_step.enter(&inner.scope());
                let blank_step = blank_step.enter(&inner.scope());

                inner.map(|(from, via)| (via, from))
                     .join_map(&blank_step, |_via, &from, &to| (from, to))
                     .concat(&value_step)
                     .distinct()
            });

            // value_blank_star.map(|_|()).consolidate().inspect(|x| println!("vbs: {:?}", x.2));

            // Paths of the form value (blank)^* value.
            let next_visible =
            value_blank_star
                .map(|(prev, next)| (next,prev))
                .semijoin(&has_value)
                .map(|(next,prev)| (prev,next));

            let result =
            next_visible
                .map(|(prev,next)| (next,prev))
                .join_map(&current_value, |next, prev, value| (prev.0, next.0, value.clone()));

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
        // println!("{:?}", timer.elapsed());

        while worker.step() {
        }
        println!("{:?}\tcomplete", timer.elapsed());

    }).unwrap();
}
