/* eslint no-console:"off" */
"use string";

require("colors");
const async = require("async");
const _ = require("underscore");
const assert = require("better-assert");

const opcua = require("node-opcua");
const DataType = opcua.DataType;
const ec =  opcua.basic_types;//xx require("node-opcua-basic-types");

//const hostname = require("os").hostname();
const hostname = "MYSERVER";


const doDebug =false;
const maxByteStringLength = 60;
const arraySize = 30;

const nbReadValuePerRequest = 15;
///const concurrencies =[1,4,8,16,32,256,1024];
const concurrencies =[1,4,8,16,32,256,512];//4,8,16,32,256,1024];

const refNodes2 = [
    "scalar_boolean",
    "scalar_sbyte",
    "scalar_int16",
    "scalar_int32",
    "scalar_int64",
    "scalar_byte",
    "scalar_uint16",
    "scalar_uint32",
    "scalar_uint64",
    "scalar_float",
    "scalar_double",
    "scalar_string",
    "scalar_datetime",
    "scalar_guid",
    "scalar_bytestring",
    "scalar_xmlelement",
    "scalar_localizedtext",
    "scalar_qualifiedname",
    "array_boolean",
    "array_sbyte",
    "array_int16",
    "array_int32",
    "array_int64",
    "array_byte",
    "array_uint16",
    "array_uint32",
    "array_uint64",
    "array_float",
    "array_double",
    "array_string",
    "array_datetime",
    "array_guid",
    "array_bytestring",
    "array_xmlelement",
    "array_localizedtext",
    "array_qualifiedname"
];
const refNodes1 = [
    "scalar_boolean",
    "scalar_sbyte",
    "scalar_int16",
    "scalar_int32",
    "scalar_int64",
    "scalar_byte",
    "scalar_uint16",
    "scalar_uint32",
    "scalar_uint64",
    "scalar_float",
    "scalar_double",
    "scalar_string",
    "scalar_datetime",
//  "scalar_guid",
    "scalar_bytestring",
//    "scalar_xmlelement",
//    "scalar_localizedtext",
//    "scalar_qualifiedname",
/*
    "array_boolean",
    "array_sbyte",
    "array_int16",
    "array_int32",
    "array_int64",
    "array_byte",
    "array_uint16",
    "array_uint32",
    "array_uint64",
    "array_float",
    "array_double",
    "array_string",
    "array_datetime",
    "array_guid",
    "array_bytestring",
    "array_xmlelement",
    "array_localizedtext",
    "array_qualifiedname"
*/
];
const g_results ={};

function benchmark(name,nodes, endpointUrl, callback) {

    assert(nodes instanceof Object);

    const securityMode = opcua.MessageSecurityMode.NONE;
    const securityPolicy = opcua.SecurityPolicy.None;
    //Xx const securityMode = opcua.MessageSecurityMode.SIGNANDENCRYPT;
    //Xx const securityPolicy = opcua.SecurityPolicy.Basic256;

    const options = {
        securityMode: securityMode,
        securityPolicy: securityPolicy,
        connectionStrategy: {
            maxRetry: 0
        },
        endpoint_must_exist: false
    };
    const client = new opcua.OPCUAClient(options);
    let the_session;


    var nodesToRead;

    //xx console.log(nodesToRead);

    let read_counter = 0;
    let error_counter = 0;
    let stats = [];

    function single_read(callback) {
        read_counter += 1;
        const n = read_counter;
        const hrTime = process.hrtime();
        //console.log("--------------<<<<<<<<<<<<", n)
        the_session.read(nodesToRead, function (err,dataValues) {
            const diff = process.hrtime(hrTime); // second nano seconds
            const time_in_sec = diff[0] + diff[1] / 1000000000.0; // in seconde
            const ops_per_sec = nodesToRead.length / time_in_sec;
            stats.push(time_in_sec);
            if (err) {
                console.log("Error !",err.message);
                error_counter++;
                return callback(err);
            }
            _.zip(nodesToRead, dataValues).forEach(function (pair) {
                const nodeId = pair[0].nodeId;
                const result = pair[1];
                if (result.statusCode != opcua.StatusCodes.Good) {
                    console.log("  nodeId ", nodeId.toString(), " => ", result.statusCode
                        .toString());
                    process.exit();
                }
            });
            if (doDebug) {
                console.log("err = ", err, diff, dataValues.length, n, time_in_sec,
                    " ops per sec = ", ops_per_sec);
            }
            callback();
        });
    }

 
    function performance_testing(callback) {
        const tasks = concurrencies.map(n=>performance_testing_parallel.bind(null,n));
        async.series(tasks, callback);
    }

    function performance_testing_parallel(nbConcurrentRead, callback) {

        stats = [];
        const tasks = [];
        const nb_reads = 2048;
        for (let i = 0; i < nb_reads; i++) {
            tasks.push(single_read);
        }

        const b_bytesWritten= client.bytesWritten;
        const b_bytesRead= client.bytesRead;
        const b_transactionsCount= client.transactionsPerformed;
        const b_chunkWrittenCount= client._secureChannel._transport.chunkWrittenCount;
        const b_chunkReadCount= client._secureChannel._transport.chunkReadCount;

        const hrTime = process.hrtime();
        async.parallelLimit(tasks, nbConcurrentRead, function (err) {

            if (err) {
                console.log("Err => ",err);
                return callback();
            }
            const diff = process.hrtime(hrTime);
            const total_time2 = diff[0] + diff[1] / 1000000000.0; // in seconds
            const total_time = stats.reduce(function (a, b) {
                return a + b;
            }, 0);

            const ops_per_sec = nodesToRead.length * nb_reads / total_time;
            const ops_per_sec2 = nodesToRead.length * nb_reads / total_time2;

            const data = {
                nbConcurrentRead: nbConcurrentRead,
                nbReadPerSecLocal: ops_per_sec,
                nbReadPerSecGlobal: ops_per_sec2,
                readTimeLocal: total_time,
                readTimeGlobal: total_time2,
                bytesWritten: client.bytesWritten - b_bytesWritten,
                bytesRead: client.bytesRead - b_bytesRead,
                transactionsCount: client.transactionsPerformed - b_transactionsCount,
                chunkWrittenCount: client._secureChannel._transport.chunkWrittenCount -b_chunkWrittenCount,
                chunkReadCount: client._secureChannel._transport.chunkReadCount - b_chunkReadCount
            };

            console.log("   nbConcurrentRead = ", nbConcurrentRead);
            console.log("         local nb read per sec = ".cyan, 
                data.nbReadPerSecLocal.toFixed(2).white.bold, 
                " apparent time= ", data.readTimeLocal);
            console.log("         overall read  per sec = ".cyan, 
                data.nbReadPerSecGlobal.toFixed(2).white.bold,
                " t = ", data.readTimeGlobal);
            console.log("                      byteWritten = ".cyan, data.bytesWritten);
            console.log("                      byteRead    = ".cyan, data.bytesRead);
            console.log("            transactionsPerformed = ".cyan, data.transactionsCount);
            console.log("            chunkWrittenCount     = ".cyan, data.chunkWrittenCount);
            console.log("            chunkReadCount        = ".cyan, data.chunkReadCount);
           
            g_results[name] =  g_results[name] || {};
            g_results[name][nbConcurrentRead] = data;
            callback();
        });
    }

    async.series([

        // step 1 : connect to
        function (callback) {

            client.on("start_reconnection", function () {
                console.log(" ... start_reconnection");
            });
            client.on("backoff", function (nb, delay) {
                console.log("  connection failed for the", nb,
                    " time ... We will retry in ", delay, " ms");
            });
            client.connect(endpointUrl, function (err) {
                if (err) {
                    console.log(" cannot connect to endpoint :".cyan, endpointUrl);
                    console.log(" ERR = >",err.message);
                } else {
                    if (doDebug) {
                        console.log("connected !");
                    }
                }
                callback(err);
            });
        },

        // step 2 : createSession
        function (callback) {
            client.createSession(function (err, session) {
                if (!err) {
                    the_session = session;
                }
                callback(err);
            });
        },

        // step2 a: register the node that we will use to reduce the memory overhead
        //          in data transfer
        function (callback) {

            var variable_names = Object.keys(nodes);
            var nodesToRegister = variable_names.map(name=>nodes[name]);
        
            the_session.registerNodes(nodesToRegister,function(err,aliasNodes){
                
                if (err) { return callback(err); }
                
                _.zip(variable_names,aliasNodes).map(([name,alias])=>{
                    nodes[name] = alias;
                });

                nodesToRead = variable_names.map(function (k) {
                    return {
                        nodeId:  nodes[k],
                        attributeId: opcua.AttributeIds.Value
                    };
                });

                //console.log(nodesToRead.map(x=>x.nodeId.toString()).join(" "));
                //console.log(aliasNodes.map(x=>x.toString()).join(" "));

                while(nodesToRead.length<nbReadValuePerRequest)  {
                    nodesToRead = [].concat(nodesToRead,nodesToRead);
                }
                
                console.log("Nodes to Read => ", nodesToRead.length);
            

                callback();
            });
        },

        // step3 : initialize node to known values
        function (callback) {
            init_nodes(the_session, nodes, callback);
        },

        performance_testing,


        function (callback) {
            the_session.close(callback);
        },
        function (callback) {
            client.disconnect(callback);
        }
    ], function (err) {
        if (doDebug) {
            console.log(" completed");
        }
        if (err) {
            console.log("Failed with error = ", err.message);
        }
        callback();
    });
}

function filterNodes(nodes) {
    var selectedNodes ={};
    for (let i=0;i<refNodes1.length;i++) {
        const n = refNodes1[i];
        selectedNodes[n] = nodes[n]; 
    }
    return selectedNodes;
}

async.series([

    function (callback) {
        console.log(" Performance testing : NODE OPCUA");
        const nodes = filterNodes(require("./config").nodes_node_opcua);
        // let hostname = "opcuademo.sterfive.com";
        const endpointUrl = "opc.tcp://" + hostname + ":26543";
        benchmark("NodeOPCUA(0.5.4)",nodes, endpointUrl, callback);
    },

    function (callback) {
        console.log(" Performance testing : NODE OPCUA old");
        const nodes = filterNodes(require("./config").nodes_node_opcua);
        let hostname = "opcuademo.sterfive.com";
        const endpointUrl = "opc.tcp://" + hostname + ":26543";
        benchmark("NodeOPCUA(0.0.65)",nodes, endpointUrl, callback);
    },

    function (callback) {
        console.log(" Performance testing : OpenOPCUA");
        const nodes = filterNodes(require("./config").nodes_openopcua);
        const endpointUrl = "opc.tcp://" + hostname + ":16664";
        benchmark("OpenOPCUA",nodes, endpointUrl, callback);
    },
    
    function (callback) {
        console.log(" Performance testing : UA Automation CPP");
        const nodes = filterNodes(require("./config").nodes_uaautomation_cpp);
        const endpointUrl = "opc.tcp://" + hostname + ":48010";
        benchmark("UA Automation CPP",nodes, endpointUrl, callback);
    },
    function (callback) {
        console.log(" Performance testing : UA Automation AnsiC");
        const nodes = filterNodes(require("./config").nodes_uaautomation_ansiC);
        const endpointUrl = "opc.tcp://" + hostname + ":48020";
        benchmark("UA Automation AnsiC",nodes, endpointUrl, callback);
    },
    function (callback) {
        console.log(" Performance testing : PROSYS");
        const nodes = filterNodes(require("./config").nodes_prosys);
        const endpointUrl = "opc.tcp://" + hostname + ":53530/OPCUA/SimulationServer";
        benchmark("PROSYS",nodes, endpointUrl, callback);
    },
    function (callback) {
        console.log(" Performance testing : OPC Foundation generic_opc".yellow);
        const nodes = filterNodes(require("./config").generic_opc);
        const endpointUrl = "opc.tcp://" + "localhost" + ":62541/Quickstarts/ReferenceServer";
        benchmark("OPCFoundation",nodes, endpointUrl, callback);
    },
    function (callback) {
        console.log(" Performance testing : Eclipse Milo".yellow);
        const nodes = filterNodes(require("./config").nodes_milo);
        const endpointUrl = "opc.tcp://" + hostname + ":12686/example";
        benchmark("Milo",nodes, endpointUrl, callback);
    },
    function (callback) {
        console.log("Results");
        const table = [];
        // push column headers:
        table.push([ "Stack"]);
        concurrencies.forEach(c=>{
            table[0].push("Read " + c);
        });
        table[0].push("Bytes Written");
        table[0].push("Chunks Written");
        table[0].push("Bytes Read");
        table[0].push("Chunks Read");
    
        Object.keys(g_results).map(stack =>{
            const row = [stack];
            concurrencies.forEach(c=>{
                const r = g_results[stack][c];
                row.push(r ? r.nbReadPerSecGlobal.toFixed(0) : "---");
            });

            const r = g_results[stack][concurrencies[0]];
            row.push(r ? r.bytesWritten.toFixed(0) : "---");
            row.push(r ? r.chunkWrittenCount.toFixed(0) : "---");
            row.push(r ? r.bytesRead.toFixed(0) : "---");
            row.push(r ? r.chunkReadCount.toFixed(0) : "---");
            table.push(row);
        });

        var markdownTable = require("markdown-table");

        console.log(markdownTable(table,{align:"r"}));
        
        callback();
    }

], function () {
    console.log("----------------------->");
    process.exit(0);
});


const buildVariantArray = opcua.buildVariantArray;

function makeRandomArray(Type, value, n) {

    if (_.isFunction(value)) {
        value = value();
    }
    const a = buildVariantArray(DataType[Type], n, value);
    for (let i = 0; i < n; i++) {
        a[i] = value;
    }
    return a;
}

function makeDefaultArrayValue(Type, valueOrFunc, size) {
    const res = new opcua.Variant({
        dataType: DataType[Type],
        arrayType: opcua.VariantArrayType.Array,
        value: makeRandomArray(Type, valueOrFunc, size)
    });
    //assert(res.value instanceof Array);
    return res;
}

const defaultValue = {

    array_boolean: makeDefaultArrayValue("Boolean", ec.randomBoolean, arraySize),
    array_sbyte: makeDefaultArrayValue("SByte", ec.randomSByte, arraySize),
    array_int16: makeDefaultArrayValue("Int16", ec.randomInt16, arraySize),
    array_int32: makeDefaultArrayValue("Int32", ec.randomInt32, arraySize),
    array_int64: makeDefaultArrayValue("Int64", ec.randomInt64, arraySize),
    array_byte: makeDefaultArrayValue("Byte", ec.randomByte, arraySize),
    array_uint16: makeDefaultArrayValue("UInt16", ec.randomUInt16, arraySize),
    array_uint32: makeDefaultArrayValue("UInt32", ec.randomUInt32, arraySize),
    array_uint64: makeDefaultArrayValue("UInt64", ec.randomUInt64, arraySize),
    array_float: makeDefaultArrayValue("Float", ec.randomFloat, arraySize),
    array_double: makeDefaultArrayValue("Double", ec.randomFloat, arraySize),
    array_string: makeDefaultArrayValue("String", ec.randomString, arraySize),
    array_bytestring: makeDefaultArrayValue("ByteString", ec.randomByteString.bind(
        null, 10, maxByteStringLength),
    arraySize),
    array_datetime: makeDefaultArrayValue("DateTime", ec.randomDateTime,
        arraySize),
    array_guid: makeDefaultArrayValue("Guid", ec.randomGuid,
        arraySize),

    array_xmlelement: makeDefaultArrayValue("XmlElement",
        "<foo><bar></bar></foo>",
        arraySize),
    array_localizedtext: makeDefaultArrayValue("LocalizedText", opcua.coerceLocalizedText(
        "HHHH"), arraySize),
    array_qualifiedname: makeDefaultArrayValue("QualifiedName", opcua.coerceQualifyName(
        "HHHH"), arraySize),


    scalar_boolean: {
        dataType: DataType.Boolean,
        value: true
    },
    scalar_sbyte: {
        dataType: DataType.SByte,
        value: 15
    },
    scalar_int16: {
        dataType: DataType.Int16,
        value: 134
    },
    scalar_int32: {
        dataType: DataType.Int32,
        value: 12345
    },
    scalar_int64: {
        dataType: DataType.Int64,
        arrayType: opcua.VariantArrayType.Scalar,
        value: [2, 23]
    },
    scalar_byte: {
        dataType: DataType.Byte,
        value: 12
    },
    scalar_uint16: {
        dataType: DataType.UInt16,
        value: 25
    },
    scalar_uint32: {
        dataType: DataType.UInt32,
        value: 25
    },
    scalar_uint64: {
        dataType: DataType.UInt64,
        arrayType: opcua.VariantArrayType.Scalar,
        value: [242, 2323]
    },
    scalar_float: {
        dataType: DataType.Float,
        value: 3.14
    },
    scalar_double: {
        dataType: DataType.Double,
        value: 6.28
    },
    scalar_string: {
        dataType: DataType.String,
        value: "abcdefghijklmnopqrstuvwxyz"
    },
    scalar_datetime: {
        dataType: DataType.DateTime,
        value: new Date()
    },
    scalar_guid: {
        dataType: DataType.Guid,
        value: ec.randomGuid()
    },
    scalar_bytestring: {
        dataType: DataType.ByteString,
        value: ec.randomByteString(10, maxByteStringLength)
    },
    scalar_xmlelement: {
        dataType: DataType.XmlElement,
        value: "<foo></foo>"
    },
    scalar_localizedtext: {
        dataType: DataType.LocalizedText,
        value: opcua.coerceLocalizedText("Hello World")
    },
    scalar_qualifiedname: {
        dataType: DataType.QualifiedName,
        value: new opcua.QualifiedName("Hello", 1)
    }
};

function init_nodes(session, nodes, callback) {

    const nodesToWrite = Object.keys(nodes).map(function (key) {
        const v = new opcua.Variant(defaultValue[key]);
        //xxconsole.log((new opcua.Variant(v)).toString());
        const n = nodes[key];
        return new opcua.write_service.WriteValue({
            nodeId: n,
            attributeId: opcua.AttributeIds.Value,
            value: {
                value: v
            }
        });
    });

    async.series([

        function write_nodes(callback) {
            session.write(nodesToWrite, function (err, results) {
                const bad = _.zip(nodesToWrite,results).filter(pair=>pair[1] != opcua.StatusCodes.Good);
                if (bad.length>0) {
                    bad.forEach((x)=>{console.log(x[0].nodeId.toString(),x[0].value.value.toString() + x[1].toString());});
                    //xx console.log("err = ",err,bad.map(x=>" ERR=" + x[1].toString()));
                    err =new Error(" Invalid write on node of types : " + bad.map(x=>x[0].value.value.dataType.toString()).join(" "));
                }
                callback(err);
            });        
        },
        function verify_nodes(callback) {
            session.read(nodesToWrite,function(err,dataValues){
                for(let i=0;i<nodesToWrite.length && i<2;i++) {
                    console.log(nodesToWrite[i].value.value.toString(),dataValues[i].value.toString());
                }
                callback();
            });
        }
    ],callback);
}
