import G6 from "@antv/g6";

function initG6Graph(container) {
    return new G6.Graph(
        {
            container: container,
            fitView: true,
            // // 上，右，下，左
            // fitViewPadding: [10,10,-1,10],
            groupByTypes: false,
            defaultEdge: {
                style: {
                    stroke: '#ff000e',
                    lineWidth: 2,
                    endArrow: true,
                },
            },
            defaultNode: {
                type: 'rect',
                size: [180, 30],
                anchorPoints: [
                    [0.5, 0],
                    [0.5, 1],
                ],
            },
            defaultCombo: {
                type: 'rect',
                anchorPoints: [
                    [0.5, 0],
                    [0.5, 1],
                ],
                padding: [20, 20, 10, 20],
            },
            layout: {
                type: 'dagreCompound',
                rankdir: 'TD',
                ranksep: 20,
            },
            modes: {
                default: [
                    {
                        type: 'tooltip', // 提示框
                        formatText(model) {
                            // 提示框文本内容
                            return model.tip;
                        },
                    },
                ],
            },
        }
    );
}

export default initG6Graph;