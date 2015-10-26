use petgraph::{Graph, Directed};
use petgraph::graph::NodeIndex;
use utils::ObjRef;

pub type Host = u64;

// A node in the computation graph, can be a data node (Obj), a function call node (Op), a Map node
// or a Reduce node. An opid is a pointer into the ops vector of the computation graph.
#[derive(Hash, PartialOrd, Ord, PartialEq, Eq, Clone, Copy, Debug)]
enum Node<'a> {
    // Map {
    //    opid: usize
    // },
    Reduce {
        opid: usize
    },
    Op {
        opid: usize
    },
    Obj {
        objref: ObjRef,
        hosts: &'a[Host]
    }
}

pub struct CompGraph<'a> {
    objs: Vec<NodeIndex>, // mapping from objrefs to nodes in the graph
    ops: Vec<String>, // names of operations
    graph: Graph<Node<'a>, f32, Directed> // computation graph
}

impl<'a> CompGraph<'a> {
    pub fn new() -> CompGraph<'a> {
        return CompGraph {
            graph: Graph::new(),
            objs: Vec::new(),
            ops: Vec::new()
        };
    }
    pub fn add_obj(self: &mut CompGraph<'a>) -> (ObjRef, NodeIndex) {
        let objref = self.objs.len();
        let obj = self.graph.add_node(Node::Obj{objref: objref, hosts: &[]});
        self.objs.push(obj);
        return (objref, obj);
    }
    pub fn add_op<'b>(self: &mut CompGraph<'a>, name: String, args: &'b [ObjRef], result: ObjRef) {
        self.ops.push(name);
        let func = self.graph.add_node(Node::Op {opid: self.ops.len() - 1});
        let res = self.graph.add_node(Node::Obj {objref: result, hosts: &[]});
        for arg in args {
            self.graph.add_edge(self.objs[*arg], func, 0.0);
        }
        self.graph.add_edge(func, res, 0.0);
    }
    // pub fn add_map<'b>(self: &mut CompGraph<'a>, name: String, args: &'b [ObjRef]) -> &'a[ObjRef] {
    //
    // }
    pub fn add_reduce<'b>(self: &mut CompGraph<'a>, name: String, args: &'b [ObjRef], result: ObjRef) {
        self.ops.push(name);
        let reduce = self.graph.add_node(Node::Reduce {opid: self.ops.len() - 1});
        let res = self.graph.add_node(Node::Obj {objref: result, hosts: &[]});
        for arg in args {
            self.graph.add_edge(self.objs[*arg], reduce, 0.0);
        }
        self.graph.add_edge(reduce, res, 0.0);
    }
}

pub struct DotBuilder {
    buf: String,
}

impl DotBuilder {
    pub fn new_digraph(name: &str) -> Self {
        DotBuilder{buf: format!("digraph \"{}\" {}", name, "{\n")}
    }

    pub fn set_node_attrs(&mut self, node: &str, attrs: &str) {
        self.buf.push_str(&format!("\"{}\" [{}];\n", node, attrs));
    }

    pub fn add_edge(&mut self, from: &str, to: &str) {
        self.buf.push_str(&format!("\"{}\" -> \"{}\";\n", from, to));
    }

    pub fn finish(&mut self) {
        self.buf.push_str("}\n");
    }
}

pub fn to_dot<'a>(graph: &CompGraph<'a>) -> String {
    let mut builder = DotBuilder::new_digraph("");
    for i in 0..graph.graph.node_count() {
        let idx = NodeIndex::new(i);
        let id = i.to_string();
        let weight = graph.graph.node_weight(idx).unwrap();
        let label = match *weight {
            Node::Op { opid } => format!("label=\"{}\"", &graph.ops[opid]),
            Node::Obj { objref, hosts } => format!("label=\"{}\"", objref),
            Node::Reduce { opid } => format!("label=\"reduce {}\"", &graph.ops[opid])
        };
        builder.set_node_attrs(&id, &label);
    }
    for edge in graph.graph.raw_edges() {
        let src = edge.source().index().to_string();
        let target = edge.target().index().to_string();
        builder.add_edge(&src[..], &target[..]);
    }
    builder.finish();
    return builder.buf;
}
