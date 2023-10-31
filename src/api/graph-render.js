function graphRender(graph, graphData) {

    graph.clear();
    graph.data({
        nodes: graphData.nodes,
        edges: graphData.edges,
        combos: graphData.combos,
    });


    graph.render();

    for (let i = 0; i < graphData.nodesToHide.length; i++) {
        graph.findById(graphData.nodesToHide[i]).hide();
    }

    for (let i = 0; i < graphData.edgesToHide.length; i++) {
        graph.findById(graphData.edgesToHide[i]).hide();
    }


}

export default graphRender;