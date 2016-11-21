"use string";

require("colors");
var opcua = require("node-opcua");
var async = require("async");
var _ = require("underscore");
var assert = require("better-assert");

var hostname = require("os").hostname();

var ec = opcua.encode_decode;


var doDebug = false;
var maxByteStringLength = 60;
var arraySize = 30;

var DataType = opcua.DataType;


function benchmark(nodes, endpointUrl, callback) {

    var securityMode = opcua.MessageSecurityMode.NONE;
    var securityPolicy = opcua.SecurityPolicy.None;
    //xx var securityMode = opcua.MessageSecurityMode.SIGNANDENCRYPT;
    //xx var securityPolicy = opcua.SecurityPolicy.Basic128Rsa15;

    var options = {
        securityMode: securityMode,
        securityPolicy: securityPolicy,
        connectionStrategy: {
            maxRetry: 1
        }
    };
    var client = new opcua.OPCUAClient(options);
    var the_session;


    var nodesToRead = Object.keys(nodes).map(function (k) {
        var v = nodes[k];
        //console.log( "v=",k);
        return {
            nodeId: opcua.resolveNodeId(v),
            attributeId: opcua.AttributeIds.Value
        };

    });
    //xx console.log(nodesToRead);

    var read_counter = 0;

    var error_counter = 0;
    var stats = [];

    function single_read(callback) {
        read_counter += 1
        var n = read_counter;
        var hrTime = process.hrtime();
        //console.log("--------------<<<<<<<<<<<<", n)
        the_session.read(nodesToRead, function (err, a, results) {
            var diff = process.hrtime(hrTime); // second nano seconds
            var time_in_sec = diff[0] + diff[1] / 1000000000.0; // in seconde
            var ops_per_sec = nodesToRead.length / time_in_sec;
            stats.push(time_in_sec);
            if (err) {
                error_counter++;
                return callback(err);
            }
            _.zip(nodesToRead, results).forEach(function (pair) {
                var nodeId = pair[0].nodeId;
                var result = pair[1];
                if (result.statusCode != opcua.StatusCodes.Good) {
                    console.log("  nodeId ", nodeId.toString(), " => ", result.statusCode
                        .toString());
                    process.exit();
                }
            });
            if (doDebug) {
                console.log("err = ", err, diff, results.length, n, time_in_sec,
                    " ops per sec = ", ops_per_sec);
            }
            callback();
        });
    }

    function performance_testing(callback) {

        async.series([
            performance_testing_parallel.bind(null, 1),
            performance_testing_parallel.bind(null, 4),
            performance_testing_parallel.bind(null, 16),
            performance_testing_parallel.bind(null, 256),
            performance_testing_parallel.bind(null, 1024),
            //xx performance_testing_parallel.bind(null, 10),
            //xx performance_testing_parallel.bind(null, 100),
        ], callback);

    }

    function performance_testing_parallel(nbConcurrentRead, callback) {

        var tasks = [];
        var nb_reads = 2048;
        for (var i = 0; i < nb_reads; i++) {
            tasks.push(single_read);
        }

        var hrTime = process.hrtime();
        async.parallelLimit(tasks, nbConcurrentRead, function (err) {

            if (err) {
                console.log("Err => ",err);
                return callback();
            }
            var diff = process.hrtime(hrTime);

            var total_time2 = diff[0] + diff[1] / 1000000000.0; // in seconds
            var total_time = stats.reduce(function (a, b) {
                return a + b;
            }, 0);

            var ops_per_sec = nodesToRead.length * nb_reads / total_time;
            var ops_per_sec2 = nodesToRead.length * nb_reads / total_time2;

            console.log("   nbConcurrentRead = ", nbConcurrentRead);
            console.log("         local nb read per sec = ".cyan, ops_per_sec.toFixed(2).white.bold, " apparent time= ", total_time);
            console.log("         overall read  per sec = ".cyan, ops_per_sec2.toFixed(2).white.bold, " t = ", total_time2);
            console.log("                      byteWritten = ".cyan, client.bytesWritten);
            console.log("                      byteRead    = ".cyan, client.bytesRead);
            console.log("            transactionsPerformed = ".cyan, client.transactionsPerformed);
            console.log("            chunkWrittenCount     = ".cyan, client._secureChannel._transport.chunkWrittenCount);
            console.log("            chunkReadCount        = ".cyan, client._secureChannel._transport.chunkReadCount);
            callback();
        });
    }

    async.series([

        // step 1 : connect to
        function (callback) {

            client.on("start_reconnection", function () {
                console.log(" ... start_reconnection")
            });
            client.on("backoff", function (nb, delay) {
                console.log("  connection failed for the", nb,
                    " time ... We will retry in ", delay, " ms");
            });
            client.connect(endpointUrl, function (err) {
                if (err) {
                    console.log(" cannot connect to endpoint :", endpointUrl);
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
            console.log("Failed with error = ", err);
        }
        callback();
    });
}


async.series([


    function (callback) {
        var node, endpointUrl;
        console.log(" Performance testing : NODE OPCUA");
        nodes = require("./config").nodes_node_opcua;
        endpointUrl = "opc.tcp://" + hostname + ":26543";
        benchmark(nodes, endpointUrl, callback);
    },
    function (callback) {
        var node, endpointUrl;
        console.log(" Performance testing : UA Automation");
        nodes = require("./config").nodes_uaautomation_cpp;
        endpointUrl = "opc.tcp://" + hostname + ":48010";
        benchmark(nodes, endpointUrl, callback);
    },
    function (callback) {
        var node, endpointUrl;
        console.log(" Performance testing : PROSYS");
        nodes = require("./config").nodes_prosys;
        endpointUrl = "opc.tcp://" + hostname + ":53530/OPCUA/SimulationServer";
        benchmark(nodes, endpointUrl, callback);
    },
    function (callback) {
        var node, endpointUrl;
        console.log(" Performance testing : OPC Foundation generic_opc");
        nodes = require("./config").generic_opc;
        endpointUrl = "opc.tcp://" + "localhost" + ":62541/Quickstarts/ReferenceServer";
        benchmark(nodes, endpointUrl, callback);
    },
    function (callback) {
        var node, endpointUrl;
        console.log(" Performance testing : Eclipse Milo");
        nodes = require("./config").nodes_milo;
        endpointUrl = "opc.tcp://" + "localhost" + ":12686/example";
        benchmark(nodes, endpointUrl, callback);
    }


], function () {
    console.log("----------------------->");
    process.exit(0);
})


var buildVariantArray = opcua.buildVariantArray;

function makeRandomArray(Type, value, n) {

    if (_.isFunction(value)) {
        value = value();
    }
    var a = buildVariantArray(DataType[Type], n, value);
    for (var i = 0; i < n; i++) {
        a[i] = value;
    }
    return a;
}

function makeDefaultArrayValue(Type, valueOrFunc, size) {
    var res = new opcua.Variant({
        dataType: DataType[Type],
        arrayType: opcua.VariantArrayType.Array,
        value: makeRandomArray(Type, valueOrFunc, size)
    });
    //assert(res.value instanceof Array);
    return res;
}

var defaultValue = {

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
}

function init_nodes(session, nodes, callback) {

    var nodesToWrite = Object.keys(nodes).map(function (key) {
        var v = defaultValue[key];
        //xxconsole.log((new opcua.Variant(v)).toString());
        var n = nodes[key];
        return {
            nodeId: n,
            attributeId: opcua.AttributeIds.Value,
            value: {
                value: v
            }
        }
    });

    session.write(nodesToWrite, function (err, results) {
        var bad = _.zip(nodesToWrite,results).filter(function(pair,index){ return pair[1] != opcua.StatusCodes.Good;});
        if (bad.length>0) {

            console.log("err = ",err,bad);
          err =new Error(" Invalid write on node ",bad.map(Object.prototype.toString).join(""));

        }
        callback(err);
    })
}
