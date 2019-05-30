extern crate timely;
extern crate differential_dataflow;

use timely::order::Product;
use timely::dataflow::*;

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

type Node = u32;
type Time = u32;
type Iter = u32;
type Diff = isize;

fn main() {

    // start up timely computation
    timely::execute_from_args(std::env::args(), |worker| {

        // construct streaming scope
        let (mut c, mut p, mut q, mut r, mut s, mut u) =
        worker.dataflow::<Time,_,_>(move |outer| {

            // inputs for base facts; currently not used because no data on hand.
            let (_cin, c) = outer.new_collection::<(Node,Node,Node),Diff>();
            let (_pin, p) = outer.new_collection::<(Node,Node),Diff>();
            let (_qin, q) = outer.new_collection::<(Node,Node,Node),Diff>();
            let (_rin, r) = outer.new_collection::<(Node,Node,Node),Diff>();
            let (_sin, s) = outer.new_collection::<(Node,Node),Diff>();
            let (_uin, u) = outer.new_collection::<(Node,Node,Node),Diff>();

            // construct iterative derivation scope
            let (_p, _q) = outer.iterative::<Iter,_,_>(|inner| {

                // use differential_dataflow::operators::iterate;
                use differential_dataflow::operators::arrange::ArrangeByKey;
                use differential_dataflow::operators::arrange::ArrangeBySelf;

                // create new variables
                let p_var = iterate::MonoidVariable::new(inner, Product::new(Default::default(), 1));
                let q_var = iterate::MonoidVariable::new(inner, Product::new(Default::default(), 1));

                // use differential_dataflow::operators::reduce::ReduceCore;
                // use differential_dataflow::trace::implementations::ord::OrdKeySpine;
                // let p_self = p_var.reduce_abelian::<_,OrdKeySpine<_,_,_>>(move |_,_,t| t.push(((), 1)));
                // let q_self = q_var.reduce_abelian::<_,OrdKeySpine<_,_,_>>(move |_,_,t| t.push(((), 1)));

                // accumulate
                let p_new = p_var.distinct();
                let q_new = q_var.distinct();

                // arrangements for p.
                let p_by0 = p_new.arrange_by_key();
                let p_by1 = p_new.map_in_place(|(x,y)| std::mem::swap(x,y)).arrange_by_key();
                let p_by01 = p_new.arrange_by_self();   // TODO: Could be shared with the `distinct`.

                // arrangements for q.
                let q_by0 = q_new.map(|(x,y,z)| (x,(y,z))).arrange_by_key();
                let q_by1 = q_new.map(|(x,y,z)| (y,(x,z))).arrange_by_key();
                let q_by01 = q_new.map(|(x,y,z)| ((x,y),z)).arrange_by_key();
                let q_by21 = q_new.map(|(x,y,z)| ((z,y),x)).arrange_by_key();

                // static relations from outside the iterative scope.
                let c_by1 = c.enter(inner).map(|(x,y,z)| (y,(x,z))).arrange_by_key();
                let r_by0 = r.enter(inner).map(|(x,y,z)| (x,(y,z))).arrange_by_key();
                let s_by0 = s.enter(inner).arrange_by_key();
                let u_by0 = u.enter(inner).map(|(x,y,z)| (x,(y,z))).arrange_by_key();

                // IR1: p(x,z) := p(x,y), p(y,z)
                let ir1 = p_by1.join_core(&p_by0, |_y,&x,&z| Some((x,z)));

                // IR2: q(x,r,z) := p(x,y), q(y,r,z)
                let ir2 = p_by1.join_core(&q_by0, |_y,&x,&(r,z)| Some((x,r,z)));

                // IR3: p(x,z) := p(y,w), u(w,r,z), q(x,r,y)
                let ir3 = p_by1.join_core(&u_by0, |_w,&y,&(r,z)| Some(((y,r),z)))
                               .join_core(&q_by21, |_yr,&z,&x| Some((x,z)));

                // IR4: p(x,z) := c(y,w,z), p(x,w), p(x,y)
                let ir4 = c_by1.join_core(&p_by1, |_w,&(y,z),&x| Some(((x,y),z)))
                               .join_core(&p_by01, |&(x,_y),&z,&()| Some((x,z)));

                // IR5: q(x,q,z) := q(x,r,z), s(r,q)
                let ir5 = q_by1.join_core(&s_by0, |_r,&(x,z),&q| Some((x,q,z)));

                // IR6: q(x,e,o) := q(x,y,z), r(y,u,e), q(z,u,o)
                let ir6 = q_by1.join_core(&r_by0, |_y,&(x,z),&(u,e)| Some(((z,u),(x,e))))
                               .join_core(&q_by01, |_zu,&(x,e),&o| Some((x,e,o)));

                p_var.set(&p.enter(inner).concatenate(vec![ir1, ir3, ir4]));
                q_var.set(&q.enter(inner).concatenate(vec![ir2, ir5, ir6]));

                // return the derived p and q
                (p_new.leave(), q_new.leave())
            });

            (_cin, _pin, _qin, _rin, _sin, _uin)
        });

        let prefix = std::env::args().nth(1).expect("must specify path prefix");

        for (x,y,z) in load3(worker.index(), &prefix, "c.txt") { c.insert((x,y,z)); }
        for (x,y) in load2(worker.index(), &prefix, "p.txt") { p.insert((x,y)); }
        for (x,y,z) in load3(worker.index(), &prefix, "q.txt") { q.insert((x,y,z)); }
        for (x,y,z) in load3(worker.index(), &prefix, "r.txt") { r.insert((x,y,z)); }
        for (x,y) in load2(worker.index(), &prefix, "s.txt") { s.insert((x,y)); }
        for (x,y,z) in load3(worker.index(), &prefix, "u.txt") { u.insert((x,y,z)); }

    }).unwrap();
}

fn read_file(filename: &str) -> impl Iterator<Item=String> {
    use ::std::io::{BufReader, BufRead};
    use ::std::fs::File;
    let file = BufReader::new(File::open(filename).unwrap());
    file.lines()
        .filter_map(|line| line.ok())
}

fn load2<'a>(index: usize, prefix: &str, filename: &str) -> impl Iterator<Item=(Node, Node)>+'a {
    read_file(&format!("{}{}", prefix, filename))
        .filter(move |_| index == 0)
        .map(move |line| {
            let mut elts = line.split(',');
            (
                elts.next().unwrap().parse().unwrap(),
                elts.next().unwrap().parse().unwrap(),
            )
        })
}

fn load3<'a>(index: usize, prefix: &str, filename: &str) -> impl Iterator<Item=(Node, Node, Node)>+'a {
    read_file(&format!("{}{}", prefix, filename))
        .filter(move |_| index == 0)
        .map(move |line| {
            let mut elts = line.split(',');
            (
                elts.next().unwrap().parse().unwrap(),
                elts.next().unwrap().parse().unwrap(),
                elts.next().unwrap().parse().unwrap(),
            )
        })
}
