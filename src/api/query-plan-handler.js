const StageType = {
    LOCALMR: "Map Reduce Local Work",
    MR: "Map Reduce",
    SPARK: "Spark",
    CONDITIONAL: "Conditional Operator",
    MOVE: "Move Operator",
    STATS: "Stats Work",
    FETCH: "Fetch Operator",
    TEZ: "Tez",
    DEPCOL: "Dependency Collection",
}

const TaskType = {
    MAPOPTREE: "Map Operator Tree",
    REDUCEOPTREE: "Reduce Operator Tree",
    MAPLOCALOPTREE: "Map Local Operator Tree",
    MAP: "Map",
    REDUCE: "Reducer",
    CONDITIONAL: "Conditional Operator",
    MERGE: "Spark Merge File Work",
    MOVE: "Move Operator",
    STATS: "Stats Work",
    FETCH: "Fetch Operator",
    DEPCOL: "Dependency Collection",
    FILEMERGE: "File Merge",
    FILTER: "Filter Operator",
    SparkHashTable: "Spark HashTable Sink Operator",
    FileOutput: "File Output Operator",
    MapJoin: "Map Join Operator",
    PTF: "PTF Operator",
    UNION :"Union"
}

const OperatorType = {
    TABLESCAN: "TableScan",
    REDUCEOUTPUT: "Reduce Output Operator",
    GROUPBY: "Group By Operator",
    SELECTOP: "Select Operator",
    FileOutput: "File Output Operator",
    FILTER: "Filter Operator",
}

let stageMap = new Map();
let taskMap = new Map();
let operatorMap = new Map();

//全局变量用于组成OperatorId
let operatorSequenceId = 0;


class Operator {
    /**
     * @param {String} id
     * @param {String} label
     * @param {Array} children
     * @param {Task} task
     * @param {Stage} stage
     */
    constructor(id, label, task, stage, tip, children) {
        this.id = id;
        this.label = label;
        this.task = task;
        this.stage = stage;
        this.tip = tip;
        this.children = children;
    }
}

class Task {
    /**
     * @param {String} id
     * @param {String} label
     * @param {Stage} stage
     * @param {String} type
     * @param {Object} operatorTree
     * @param {Array} children
     * @param {Operator} headOperator
     * @param {Operator} tailOperator
     */
    constructor(id, label, stage, type, operatorTree, children, headOperator, tailOperator) {
        this.id = id;
        this.label = label;
        this.stage = stage;
        this.type = type;
        this.operatorTree = operatorTree;
        this.children = children;
        this.headOperator = headOperator;
        this.tailOperator = tailOperator;
    }
}

class Stage {
    /**
     *
     * @param {String} id
     * @param {String} label
     * @param {String} type
     * @param {Object} plan
     * @param {Array} children
     * @param {Array} headTasks
     * @param {Array} tailTasks
     */
    constructor(id, label, type, plan, children, headTasks, tailTasks) {
        this.id = id;
        this.label = label;
        this.type = type;
        this.plan = plan;
        this.children = children;
        this.headTasks = headTasks;
        this.tailTasks = tailTasks;
    }
}

function getTipText(tip) {
    if (Object.keys(tip).length === 0) {
        return null;
    }

    let text = '';
    for (let key in tip) {
        text = text + key + " : " + tip[key] + "<br/>"
    }

    return text;
}


/**
 * 处理task之间的依赖关系并找到headTasks和tailTasks
 * @param {Stage} stage
 * @param {Object} tasks
 * @param {Object} taskDependencies
 */
function handleTaskDependencies(stage, tasks, taskDependencies) {

    if ((stage.type === StageType.SPARK || stage.type === StageType.TEZ) && typeof (taskDependencies) != 'undefined') {
        let deps = [];
        for (let dependency in taskDependencies) {
            if (taskDependencies[dependency] instanceof Array) {
                for (let i = 0; i < taskDependencies[dependency].length; i++) {
                    deps.push({
                        [dependency]: taskDependencies[dependency][i]['parent']
                    })
                }
            } else {
                deps.push({
                    [dependency]: taskDependencies[dependency]['parent']
                });
            }
        }
        //处理tasks之间的依赖关系
        for (let i = 0; i < deps.length; i++) {
            let sourceTaskLabel = Object.values(deps[i])[0];
            let targetTaskLabel = Object.keys(deps[i])[0];
            let sourceTaskId = stage.id + '-' + sourceTaskLabel;
            let targetTaskId = stage.id + '-' + targetTaskLabel;
            let sourceTask = taskMap.get(sourceTaskId);
            let targetTask = taskMap.get(targetTaskId);

            sourceTask.children.push(targetTask);

            let sourceOperator = sourceTask.tailOperator;
            let targetOperator = targetTask.headOperator;
            sourceOperator.children.push(targetOperator);
        }


        //找到headTasks和tailTasks
        let sourceTaskLabels = Array.from(new Set(deps.map(dep => Object.keys(dep)[0])));
        let targetTaskLabels = Array.from(new Set(deps.map(dep => Object.values(dep)[0])));


        let tailTaskLabels = sourceTaskLabels.filter(child => !targetTaskLabels.includes(child));
        let headTaskLabels = targetTaskLabels.filter(parent => !sourceTaskLabels.includes(parent));


        for (let i = 0; i < headTaskLabels.length; i++) {
            let taskId = stage.id + '-' + headTaskLabels[i];
            stage.headTasks.push(taskMap.get(taskId));
        }

        for (let i = 0; i < tailTaskLabels.length; i++) {
            let taskId = stage.id + '-' + tailTaskLabels[i];
            stage.tailTasks.push(taskMap.get(taskId));
        }

    } else if (stage.type === StageType.MR && Object.keys(tasks).includes('Map Operator Tree') && Object.keys(tasks).includes('Reduce Operator Tree')) {
        let sourceTaskId = stage.id + '-' + 'Map Operator Tree';
        let targetTaskId = stage.id + '-' + 'Reduce Operator Tree';

        let sourceTask = taskMap.get(sourceTaskId);
        let targetTask = taskMap.get(targetTaskId);

        //处理Map 和 Reduce之间的依赖关系
        sourceTask.children.push(targetTask);

        //找到Stage的headTask和tailTask
        stage.headTasks.push(sourceTask);
        stage.tailTasks.push(targetTask);

        let sourceOperator = sourceTask.tailOperator;
        let targetOperator = targetTask.headOperator;
        sourceOperator.children.push(targetOperator);
    } else {
        //没有依赖关系，所有tasks即作为headTask又作为tailTask
        for (let task in tasks) {
            let taskId = stage.id + '-' + task.replace(/:/, '').trim();
            stage.headTasks.push(taskMap.get(taskId));
            stage.tailTasks.push(taskMap.get(taskId));
        }
    }
}


/**
 * 添加Stage之间的依赖关系以及跨Stage的Operator之间的依赖关系
 * @param {Stage} sourceStage
 * @param {Stage} targetStage
 */
function handleStageDependency(sourceStage, targetStage) {
    //添加Stage之间的依赖关系
    sourceStage.children.push(targetStage);

    let sourceTasks = sourceStage.tailTasks;
    let targetTasks = targetStage.headTasks;

    //添加跨Stage的Operator之间的依赖关系
    for (let i = 0; i < sourceTasks.length; i++) {
        let sourceTask = sourceTasks[i];
        for (let j = 0; j < targetTasks.length; j++) {
            let targetTask = targetTasks[j];
            let sourceOperator = sourceTask.tailOperator;
            let targetOperator = targetTask.headOperator;
            sourceOperator.children.push(targetOperator);
        }
    }
}

function getOperatorTip(operator) {
    console.log(operator);
    let type = Object.keys(operator)[0];
    let tip = {};

    // eslint-disable-next-line no-prototype-builtins
    if (operator[type].hasOwnProperty('Statistics:')) {
        tip['Statistics'] = operator[type]['Statistics:'];
    }

    switch (type) {
        case OperatorType.TABLESCAN:
            tip['alias'] = operator[type]['alias:'];
            tip['columns'] = operator[type]['columns:'];
            break;
        case OperatorType.REDUCEOUTPUT:
            tip['Map-reduce partition columns'] = operator[type]['Map-reduce partition columns:'];
            break;
        case OperatorType.GROUPBY:
            tip['keys'] = operator[type]['keys:'];
            tip['aggregations'] = operator[type]['aggregations:'];
            tip['mode'] = operator[type]['mode:'];
            break;
        case OperatorType.FileOutput:
            tip['table_name'] = operator[type]['table:']['name:'];
            break;
        case OperatorType.SELECTOP:
            tip['expressions'] = operator[type]['expressions:'];
            break;
        case OperatorType.FILTER:
            tip['predicate'] = operator[type]['predicate:'];
            break;
        default:
            console.log("不支持" + type);
    }
    return getTipText(tip);
}


/**
 * 处理单个的OperatorTree
 * @param headOperator
 * @param tailOperator
 * @param operatorTree
 * @param task
 * @param stage
 */
function handleSingleOperatorTree(headOperator, tailOperator, operatorTree, task, stage) {
    if (operatorTree !== null && operatorTree !== undefined){
        let opLabel = Object.keys(operatorTree)[0];
        let opId = task.id + '-' + opLabel + '-' + operatorSequenceId++;
        let opTip = getOperatorTip(operatorTree);
        let operator = new Operator(opId, opLabel, task, stage, opTip, []);
        headOperator.children.push(operator);
        operatorMap.set(operator.id, operator);
        // eslint-disable-next-line no-prototype-builtins
        if (operatorTree[opLabel].hasOwnProperty('children')) {
            handleOperatorTree(operator, tailOperator, operatorTree[opLabel]['children'], task, stage);
        } else {
            operator.children.push(tailOperator);
        }
    }

}


/**
 * 递归处理OperatorTree
 * @param {Operator} headOperator
 * @param {Operator} tailOperator
 * @param {Object} operatorTree
 * @param {Task} task
 * @param {Stage} stage
 */
function handleOperatorTree(headOperator, tailOperator, operatorTree, task, stage) {

    if (operatorTree instanceof Array) {
        for (let i = 0; i < operatorTree.length; i++) {
            handleSingleOperatorTree(headOperator, tailOperator, operatorTree[i], task, stage);
        }
    } else {
        handleSingleOperatorTree(headOperator, tailOperator, operatorTree, task, stage);
    }
}


/**
 * 处理Task
 * @param {Stage} stage
 * @param {Task} task
 */
function handleTask(stage, task) {
    let headOpId = task.id + '-head';
    let headOperator = new Operator(headOpId, null, task, stage, {}, []);

    operatorMap.set(headOperator.id, headOperator);
    let tailOpId = task.id + '-tail';
    let tailOperator = new Operator(tailOpId, null, task, stage, {}, []);
    operatorMap.set(tailOperator.id, tailOperator);

    //处理OperatorTree
    handleOperatorTree(headOperator, tailOperator, task.operatorTree, task, stage);

    //将Task对象放入map
    task.headOperator = headOperator;
    task.tailOperator = tailOperator;

    taskMap.set(task.id, task);
}


/**
 * 获取Task类型
 * @param {String} task
 */
function getTaskType(task) {
    return task.replace(/[0-9]/, '').replace(/[0-9]/, '').trim();
}

/**
 * 获取每个Stage的tasks
 * @param {Object} stagePlan
 */
function getStageTasks(stagePlan) {
    let type = Object.keys(stagePlan)[0];
    switch (type) {
        case StageType.LOCALMR:
            return {'Map Local Operator Tree': Object.values(stagePlan[type]['Alias -> Map Local Operator Tree:'])};
        case StageType.MR:
            var allPlan = stagePlan[type];
            var tasks = {}
            var mapReduce = ['Map Operator Tree:', 'Reduce Operator Tree:'];
            Object.keys(allPlan).filter(key => mapReduce.includes(key)).forEach(key => tasks[key.replace(/:/, '').trim()] = allPlan[key]);
            return tasks;
        case StageType.SPARK:
            return stagePlan[type]['Vertices:'];
        case StageType.CONDITIONAL:
            return stagePlan;
        case StageType.FETCH:
            return stagePlan;
        case StageType.MOVE:
            return stagePlan;
        case StageType.STATS:
            return stagePlan;
        case StageType.DEPCOL:
            return stagePlan;
        case StageType.TEZ:
            return stagePlan[type]['Vertices:'];
        default:
            console.log('不支持stageType' + type);
            return null;
    }
}

/**
 * 获取每个Task的OperatorTree
 * @param {Object} task
 */
function getOperatorTree(task) {
    let taskLabel = Object.keys(task)[0];
    let taskType = getTaskType(taskLabel);
    switch (taskType) {
        case TaskType.MAPLOCALOPTREE:
            return task[taskLabel];
        case TaskType.MAPOPTREE:
            return task[taskLabel];
        case TaskType.REDUCEOPTREE:
            return task[taskLabel];
        case TaskType.MAP:
            return task[taskLabel]['Map Operator Tree:'];
        case TaskType.REDUCE:
            return task[taskLabel]['Reduce Operator Tree:'];
        case TaskType.MERGE:
            return task[taskLabel]['Map Operator Tree:'];
        case TaskType.FILEMERGE:
            return task[taskLabel]['Map Operator Tree:'];
        case TaskType.FETCH:
            return task;
        case TaskType.MOVE:
            return task;
        case TaskType.STATS:
            return task;
        case TaskType.CONDITIONAL:
            return task;
        case TaskType.DEPCOL:
            return task;
        case TaskType.FILTER:
            return task;
        case TaskType.SparkHashTable:
            return task;
        case TaskType.FileOutput:
            return task;
        case TaskType.MapJoin:
            return task;
        case TaskType.PTF:
            return task;
        case TaskType.UNION:
            return task;
        default:
            console.log('不支持' + taskType);
            return null;
    }
}


/**
 * 处理Stage
 * @param {String} stageLabel
 * @param {Object} stagePlan
 */
function handleStage(stageLabel, stagePlan) {

    //获取tasks
    let tasks = getStageTasks(stagePlan);

    let stageType = Object.keys(stagePlan)[0];
    // 获取taskDependencies
    let taskDependencies = stagePlan[stageType]['Edges:'];

    //创建Stage对象
    let stage = new Stage(stageLabel, stageLabel, stageType, stagePlan, [], [], []);

    //处理tasks
    for (let [key, value] of Object.entries(tasks)) {

        let taskLabel = key;
        let taskId = stageLabel + '-' + taskLabel;

        let task = {};
        task[taskLabel] = value;
        let taskType = getTaskType(taskLabel);
        let operatorTree = getOperatorTree(task);

        //创建Task对象
        let taskObj = new Task(taskId, taskLabel, stage, taskType, operatorTree, []);
        // 处理task
        handleTask(stage, taskObj);
    }

    //处理task之间的依赖关系并获取Stage的headTasks和tailTasks
    handleTaskDependencies(stage, tasks, taskDependencies);


    //将stage放入map中
    stageMap.set(stage.id, stage);
}


/**
 * 获取Node对象
 * @param {Operator} operator
 * @param {Array} nodesToHide
 */
function getNode(operator, nodesToHide) {
    if (operator.id.endsWith('head') || operator.id.endsWith('tail')) {
        nodesToHide.push(operator.id);
        return {
            id: operator.id,
            label: operator.label,
            comboId: operator.task.id,
            type: 'circle',
            size: 0.1,
        }
    } else {
        return {
            id: operator.id,
            label: operator.label,
            comboId: operator.task.id,
            tip: operator.tip,
        }
    }
}

/**
 * 获取Edge对象
 * @param {Object} parent
 * @param {Object} child
 * @param {Array} edgesToHide
 */
function getEdge(parent, child, edgesToHide) {
    if (parent.id.endsWith('head') || parent.id.endsWith('tail') || child.id.endsWith('head') || child.id.endsWith(
        'tail')) {
        edgesToHide.push(parent.id + '->' + child.id);
        return {
            id: parent.id + '->' + child.id,
            source: parent.id,
            target: child.id,
        }
    } else {
        return {
            id: parent.id + '->' + child.id,
            source: parent.id,
            target: child.id,
        }
    }
}

/**
 * 获取Task级别的combo
 * @param {Task} task
 */
function getTaskCombo(task) {
    return {
        id: task.id,
        label: task.label,
        parentId: task.stage.id,
    }
}

/**
 * 获取Stage级别的combo
 * @param {Stage} stage
 */
function getStageCombo(stage) {
    return {
        id: stage.id,
        label: stage.label,
    }
}


/**
 * 主程序入口
 * @param {Object} data
 */
function getGraphData(data) {

    //清理三个全局变量的值，为防止影响下次调用该函数的结果
    operatorMap.clear();
    taskMap.clear();
    stageMap.clear();

    let stagePlans = data["STAGE PLANS"];
    let stageDependencies = data["STAGE DEPENDENCIES"];

    //处理Stage
    for (let stagePlan in stagePlans) {
        handleStage(stagePlan, stagePlans[stagePlan]);
    }

    // 处理Stage依赖关系
    for (let dependency in stageDependencies) {
        // eslint-disable-next-line no-prototype-builtins
        if (stageDependencies[dependency].hasOwnProperty('DEPENDENT STAGES')) {
            let sourceStageIds = stageDependencies[dependency]['DEPENDENT STAGES'].split(',');
            let targetStageId = dependency;
            for (let i = 0; i < sourceStageIds.length; i++) {
                let sourceStageId = sourceStageIds[i].trim();
                let sourceStage = stageMap.get(sourceStageId);
                let targetStage = stageMap.get(targetStageId);
                handleStageDependency(sourceStage, targetStage);
            }
        }
        // eslint-disable-next-line no-prototype-builtins
        if (stageDependencies[dependency].hasOwnProperty('CONDITIONAL CHILD TASKS')) {
            let sourceStageId = dependency;
            let targetStageIds = stageDependencies[dependency]['CONDITIONAL CHILD TASKS'].split(',');
            for (let i = 0; i < targetStageIds.length; i++) {
                let targetStageId = targetStageIds[i].trim();
                let sourceStage = stageMap.get(sourceStageId);
                let targetStage = stageMap.get(targetStageId);
                handleStageDependency(sourceStage, targetStage);

            }
        }
    }

    let nodes = [];
    let edges = [];
    let combos = [];

    let nodesToHide = [];
    let edgesToHide = [];


    //获取Node和Node间的Edges
    for (let operator of operatorMap.values()) {
        nodes.push(getNode(operator, nodesToHide));

        let children = operator.children;
        for (let i = 0; i < children.length; i++) {
            edges.push(getEdge(operator, children[i], edgesToHide));
        }
    }

    //获取Combos和Combos间的Edges
    for (let task of taskMap.values()) {
        combos.push(getTaskCombo(task));

        let children = task.children;
        for (let i = 0; i < children.length; i++) {
            edges.push(getEdge(task, children[i]));
        }
    }

    //获取Stage和Stages之间的Edges
    for (let stage of stageMap.values()) {
        combos.push(getStageCombo(stage));

        let children = stage.children;
        for (let i = 0; i < children.length; i++) {
            edges.push(getEdge(stage, children[i]));
        }
    }


    return {
        nodes: nodes,
        edges: edges,
        combos: combos,
        nodesToHide: nodesToHide,
        edgesToHide: edgesToHide,
    }
}

export default getGraphData