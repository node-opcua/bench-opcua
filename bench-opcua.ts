/* eslint no-console:"off" */
"use string";

import async from "async";
import _ from "underscore";
import chalk from "chalk";
import { callbackify } from "util";
import markdownTable from "markdown-table";

import {
  OPCUAClient,
  AttributeIds,
  MessageSecurityMode,
  SecurityPolicy,
  StatusCodes,
  DataType,
  buildVariantArray,
  Variant,
  VariantArrayType,
  randomBoolean,
  randomSByte,
  randomInt16,
  randomInt32,
  randomInt64,
  randomByte,
  randomUInt16,
  randomUInt32,
  randomUInt64,
  randomFloat,
  randomString,
  randomByteString,
  randomDateTime,
  randomGuid,
  coerceLocalizedText,
  WriteValue,
  coerceQualifiedName,
  ClientSession,
  ReadValueIdOptions,
  WriteValueOptions,
} from "node-opcua";

import * as config  from "./config";

//const hostname = require("os").hostname();
const hostname = "MYSERVER";

const doDebug = false;
const maxByteStringLength = 60;
const arraySize = 30;

const nbReadValuePerRequest = 1000;
const samples = [1, 2, 4];
const _samples = [1, 2, 4, 8, 16];
//,32,256,1024];
//const samples = [1, 4, 8, 16, 32, 256, 512]; //4,8,16,32,256,1024];

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
  //"scalar_guid",
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
  "array_qualifiedname",
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
const g_results: any = {};

function sum(a: number[]) {
  return a.reduce((p, current) => p + current, 0);
}

const refNodes = refNodes2;

function filterNodes(nodes: any): Record<string, string> {
  const selectedNodes: Record<string, string> = {};
  for (let i = 0; i < refNodes.length; i++) {
    const n = refNodes[i];
    if (!nodes[n]) {
      console.log("cannot find ", n, nodes[n]);
    }
    selectedNodes[n] = nodes[n];
  }
  return selectedNodes;
}

async function createSession(endpointUrl: string): Promise<{ client: OPCUAClient; session: ClientSession }> {
  const securityMode = MessageSecurityMode.None;
  const securityPolicy = SecurityPolicy.None;

  const client = OPCUAClient.create({
    securityMode: securityMode,
    securityPolicy: securityPolicy,
    connectionStrategy: {
      maxRetry: 0,
    },
    endpoint_must_exist: false,
  });
  // step 1 : connect to
  const clientP = client as any;
  client.on("start_reconnection", () => {
    console.log(" ... start_reconnection");
  });
  client.on("backoff", function (nb, delay) {
    console.log("  connection failed for the", nb, " time ... We will retry in ", delay, " ms");
  });
  try {
    await client.connect(endpointUrl);
  } catch (err) {
    console.log(chalk.cyan(" cannot connect to endpoint :"), endpointUrl);
    console.log(" ERR = >", (err as Error).message);
    throw err;
  }
  if (doDebug) {
    console.log("connected !");
  }
  const session = await client.createSession();

  return { client, session };
}

async function makeAliases(session: ClientSession, nodes: Record<string, string>): Promise<Record<string, string>> {
  const nodesToRegister = Object.values(nodes);

  const variableNames = Object.keys(nodes);
  const newNodes: Record<string, string> = {};
  const aliasNodes = await session.registerNodes(nodesToRegister);
  _.zip(variableNames, aliasNodes).map(([name, alias]) => {
    const n = name.toString();
    newNodes[n] = alias.toString();
  });
  return newNodes;
}
async function makeNodeToRead(nodes: Record<string, string>, nbReadValuePerRequest: number): Promise<ReadValueIdOptions[]> {
  // step2 a: register the node that we will use to reduce the memory overhead
  //          in data transfer
  const variableNames = Object.keys(nodes);
  let nodesToRead: ReadValueIdOptions[];
  nodesToRead = variableNames.map((k) => ({
    nodeId: nodes[k],
    attributeId: AttributeIds.Value,
  }));

  while (nodesToRead.length < nbReadValuePerRequest) {
    nodesToRead = (<ReadValueIdOptions[]>[]).concat(nodesToRead, nodesToRead);
  }
  nodesToRead.splice(nbReadValuePerRequest);
  console.log("Nodes to Read => ", nodesToRead.length);
  return nodesToRead;
}
async function single_read2(
  session: ClientSession,
  nodesToRead: ReadValueIdOptions[],
  stats: { read_counter: number; stats: number[] }
) {
  stats.read_counter += 1;
  const n = stats.read_counter;
  const hrTime = process.hrtime();
  //console.log("--------------<<<<<<<<<<<<", n)
  const dataValues = await session.read(nodesToRead);
  const diff = process.hrtime(hrTime); // second nano seconds
  const time_in_sec = diff[0] + diff[1] / 1000000000.0; // in seconde
  const ops_per_sec = nodesToRead.length / time_in_sec;
  stats.stats.push(time_in_sec);
  _.zip(nodesToRead, dataValues).forEach(function (pair) {
    const nodeId = pair[0].nodeId;
    const result = pair[1];
    if (result.statusCode != StatusCodes.Good) {
      console.log("  nodeId ", nodeId?.toString(), " => ", result.statusCode.toString());
      process.exit();
    }
  });
  if (doDebug) {
    console.log(diff, dataValues.length, n, time_in_sec, " ops per sec = ", ops_per_sec);
  }
}
async function benchmark(name: string, nodes: Record<string, string>, endpointUrl: string): Promise<void> {
  const stats: any = {
    read_counter: 0,
    stats: [],
  };
  console.log("-------------------------------------------------------------------------------");
  console.log(" Performance testing : ", name, endpointUrl);

  async function onManySessions() {
    interface Stuff {
      client: OPCUAClient;
      session: ClientSession;
      nodesToRead: ReadValueIdOptions[];
    }
    async function performance_testing_parallel_many_client(nbConcurrentRead: number) {
      async function f(): Promise<Stuff> {
        const { session, client } = await createSession(endpointUrl);
        const aliasedNodes = await makeAliases(session, nodes);
        const nodesToRead = await makeNodeToRead(aliasedNodes, nbReadValuePerRequest);
        return { client, session, nodesToRead };
      }
      const pc: Promise<Stuff>[] = [];
      for (let i = 0; i < nbConcurrentRead; i++) {
        pc.push(f());
      }
      const cs: Stuff[] = await Promise.all(pc);

      const c_bytesWritten: number[] = [];
      const c_bytesRead: number[] = [];
      const c_transactionsCount: number[] = [];
      const c_chunkWrittenCount: number[] = [];
      const c_chunkReadCount: number[] = [];

      const nb_reads = 2048;
      const tasks1 = cs.map(async ({ client, session, nodesToRead }: Stuff, index: number) => {
        for (let i = 0; i < nb_reads; i++) {
          await single_read2(session, nodesToRead, stats);
        }
        const clientP = client as any;
        c_bytesWritten[index] = (c_bytesWritten[index] || 0) + client.bytesWritten;
        c_bytesRead[index] = (c_bytesRead[index] || 0) + client.bytesRead;
        c_transactionsCount[index] = (c_transactionsCount[index] || 0) + client.transactionsPerformed;
        c_chunkWrittenCount[index] = (c_chunkWrittenCount[index] || 0) + clientP._secureChannel._transport.chunkWrittenCount;
        c_chunkReadCount[index] = (c_chunkReadCount[index] || 0) + clientP._secureChannel._transport.chunkReadCount;
      });

      const hrTime = process.hrtime();
      await Promise.all(tasks1);
      const diff = process.hrtime(hrTime);

      const b_bytesWritten = sum(c_bytesWritten);
      const b_bytesRead = sum(c_bytesRead);
      const b_transactionsCount = sum(c_transactionsCount);
      const b_chunkWrittenCount = sum(c_chunkWrittenCount);
      const b_chunkReadCount = sum(c_chunkReadCount);

      const total_time2 = diff[0] + diff[1] / 1000000000.0; // in seconds
      const total_time = sum(stats.stats);

      const ops_per_sec = (nbReadValuePerRequest * nb_reads) / total_time;
      const ops_per_sec2 = (nbReadValuePerRequest * nb_reads) / total_time2;

      const data = {
        nbConcurrentRead: nbConcurrentRead,
        nbReadPerSecLocal: ops_per_sec,
        nbReadPerSecGlobal: ops_per_sec2,
        readTimeLocal: total_time,
        readTimeGlobal: total_time2,
        bytesWritten: b_bytesWritten,
        bytesRead: b_bytesRead,
        transactionsCount: b_transactionsCount,
        chunkWrittenCount: b_chunkWrittenCount,
        chunkReadCount: b_chunkReadCount,
      };

      console.log("   nbConcurrentRead = ", nbConcurrentRead);
      console.log(
        chalk.cyan("         local nb read per sec = "),
        data.nbReadPerSecLocal.toFixed(2),
        " apparent time= ",
        data.readTimeLocal
      );
      console.log(
        chalk.cyan("         overall read  per sec = "),
        data.nbReadPerSecGlobal.toFixed(2),
        " t = ",
        data.readTimeGlobal
      );
      console.log(chalk.cyan("                      byteWritten = "), data.bytesWritten);
      console.log(chalk.cyan("                      byteRead    = "), data.bytesRead);
      console.log(chalk.cyan("            transactionsPerformed = "), data.transactionsCount);
      console.log(chalk.cyan("            chunkWrittenCount     = "), data.chunkWrittenCount);
      console.log(chalk.cyan("            chunkReadCount        = "), data.chunkReadCount);

      g_results[name] = g_results[name] || {};
      g_results[name][nbConcurrentRead] = data;

      async function f2({ client, session }: Stuff): Promise<void> {
        await session.close();
        await client.disconnect();
      }
      const pd = cs.map(f2 as any);
      await Promise.all(pd);
    }

    for (const nbConcurrent of samples) {
      await performance_testing_parallel_many_client(nbConcurrent);
    }
  }

  async function onSingleSession() {
    const { client, session } = await createSession(endpointUrl);
    const clientP = client as any;
    await initializeNodes(session, nodes);
    const aliasedNodes = await makeAliases(session, nodes);
    const nodesToRead = await makeNodeToRead(aliasedNodes, nbReadValuePerRequest);

    async function single_read1() {
      return single_read2(session, nodesToRead, stats);
    }
    const single_read = callbackify(single_read1);

    function performance_testing_parallel(nbConcurrentRead: number, callback: () => void) {
      const tasks = [];
      const nb_reads = 2048;

      for (let i = 0; i < nb_reads; i++) {
        tasks.push(single_read);
      }

      const b_bytesWritten = client.bytesWritten;
      const b_bytesRead = client.bytesRead;
      const b_transactionsCount = client.transactionsPerformed;
      const b_chunkWrittenCount = clientP._secureChannel._transport.chunkWrittenCount;
      const b_chunkReadCount = clientP._secureChannel._transport.chunkReadCount;

      const hrTime = process.hrtime();
      async.parallelLimit(tasks, nbConcurrentRead, function (err) {
        if (err) {
          console.log("Err => ", err);
          return callback();
        }
        const diff = process.hrtime(hrTime);
        const total_time2 = diff[0] + diff[1] / 1000000000.0; // in seconds
        const total_time = sum(stats.stats);

        const ops_per_sec = (nodesToRead.length * nb_reads) / total_time;
        const ops_per_sec2 = (nodesToRead.length * nb_reads) / total_time2;

        const data = {
          nbConcurrentRead: nbConcurrentRead,
          nbReadPerSecLocal: ops_per_sec,
          nbReadPerSecGlobal: ops_per_sec2,
          readTimeLocal: total_time,
          readTimeGlobal: total_time2,
          bytesWritten: client.bytesWritten - b_bytesWritten,
          bytesRead: client.bytesRead - b_bytesRead,
          transactionsCount: client.transactionsPerformed - b_transactionsCount,
          chunkWrittenCount: clientP._secureChannel._transport.chunkWrittenCount - b_chunkWrittenCount,
          chunkReadCount: clientP._secureChannel._transport.chunkReadCount - b_chunkReadCount,
        };

        console.log("   nbConcurrentRead = ", nbConcurrentRead);
        console.log(
          chalk.cyan("         local nb read per sec = "),
          data.nbReadPerSecLocal.toFixed(2),
          " apparent time= ",
          data.readTimeLocal
        );
        console.log(
          chalk.cyan("         overall read  per sec = "),
          data.nbReadPerSecGlobal.toFixed(2),
          " t = ",
          data.readTimeGlobal
        );
        console.log(chalk.cyan("                      byteWritten = "), data.bytesWritten);
        console.log(chalk.cyan("                      byteRead    = "), data.bytesRead);
        console.log(chalk.cyan("            transactionsPerformed = "), data.transactionsCount);
        console.log(chalk.cyan("            chunkWrittenCount     = "), data.chunkWrittenCount);
        console.log(chalk.cyan("            chunkReadCount        = "), data.chunkReadCount);

        g_results[name] = g_results[name] || {};
        g_results[name][nbConcurrentRead] = data;
        callback();
      });
    }
    async function performance_testing(session: ClientSession, nodesToRead: ReadValueIdOptions[]) {
      const tasks = samples.map((n) => performance_testing_parallel.bind(null, n));

      return new Promise<void>((resolve) => {
        async.series(tasks, (err) => resolve());
      });
    }

    await performance_testing(session, nodesToRead);

    await session.close();
    await client.disconnect();
  }

  try {
    // await onManySessions();
    await onSingleSession();
  } catch (err) {
    console.log(" Cannot complete test");
  }

  if (doDebug) {
    console.log(" completed");
  }
}

function makeRandomArray(dataType: string, value: any, n: number) {
  if (_.isFunction(value)) {
    value = value();
  }
  const a = buildVariantArray((DataType as any)[dataType] as DataType, n, value);
  for (let i = 0; i < n; i++) {
    a[i] = value;
  }
  return a;
}

function makeDefaultArrayValue(Type: any, valueOrFunc: any, size: number) {
  const res = new Variant({
    dataType: DataType[Type],
    arrayType: VariantArrayType.Array,
    value: makeRandomArray(Type, valueOrFunc, size),
  });
  //assert(res.value instanceof Array);
  return res;
}

const defaultValue: Record<string, Variant> = {
  array_boolean: makeDefaultArrayValue("Boolean", randomBoolean, arraySize),
  array_sbyte: makeDefaultArrayValue("SByte", randomSByte, arraySize),
  array_int16: makeDefaultArrayValue("Int16", randomInt16, arraySize),
  array_int32: makeDefaultArrayValue("Int32", randomInt32, arraySize),
  array_int64: makeDefaultArrayValue("Int64", randomInt64, arraySize),
  array_byte: makeDefaultArrayValue("Byte", randomByte, arraySize),
  array_uint16: makeDefaultArrayValue("UInt16", randomUInt16, arraySize),
  array_uint32: makeDefaultArrayValue("UInt32", randomUInt32, arraySize),
  array_uint64: makeDefaultArrayValue("UInt64", randomUInt64, arraySize),
  array_float: makeDefaultArrayValue("Float", randomFloat, arraySize),
  array_double: makeDefaultArrayValue("Double", randomFloat, arraySize),
  array_string: makeDefaultArrayValue("String", randomString, arraySize),
  array_bytestring: makeDefaultArrayValue("ByteString", randomByteString.bind(null, 10, maxByteStringLength), arraySize),
  array_datetime: makeDefaultArrayValue("DateTime", randomDateTime, arraySize),
  array_guid: makeDefaultArrayValue("Guid", randomGuid, arraySize),

  array_xmlelement: makeDefaultArrayValue("XmlElement", "<foo><bar></bar></foo>", arraySize),
  array_localizedtext: makeDefaultArrayValue("LocalizedText", coerceLocalizedText("HHHH"), arraySize),
  array_qualifiedname: makeDefaultArrayValue("QualifiedName", coerceQualifiedName("HHHH"), arraySize),

  scalar_boolean: new Variant({
    dataType: DataType.Boolean,
    value: true,
  }),
  scalar_sbyte: new Variant({
    dataType: DataType.SByte,
    value: 15,
  }),
  scalar_int16: new Variant({
    dataType: DataType.Int16,
    value: 134,
  }),
  scalar_int32: new Variant({
    dataType: DataType.Int32,
    value: 12345,
  }),
  scalar_int64: new Variant({
    dataType: DataType.Int64,
    arrayType: VariantArrayType.Scalar,
    value: [2, 23],
  }),
  scalar_byte: new Variant({
    dataType: DataType.Byte,
    value: 12,
  }),
  scalar_uint16: new Variant({
    dataType: DataType.UInt16,
    value: 25,
  }),
  scalar_uint32: new Variant({
    dataType: DataType.UInt32,
    value: 25,
  }),
  scalar_uint64: new Variant({
    dataType: DataType.UInt64,
    arrayType: VariantArrayType.Scalar,
    value: [242, 2323],
  }),
  scalar_float: new Variant({
    dataType: DataType.Float,
    value: 3.14,
  }),
  scalar_double: new Variant({
    dataType: DataType.Double,
    value: 6.28,
  }),
  scalar_string: new Variant({
    dataType: DataType.String,
    value: "abcdefghijklmnopqrstuvwxyz",
  }),
  scalar_datetime: new Variant({
    dataType: DataType.DateTime,
    value: new Date(),
  }),
  scalar_guid: new Variant({
    dataType: DataType.Guid,
    value: randomGuid(),
  }),
  scalar_bytestring: new Variant({
    dataType: DataType.ByteString,
    value: randomByteString(10, maxByteStringLength),
  }),
  scalar_xmlelement: new Variant({
    dataType: DataType.XmlElement,
    value: "<foo></foo>",
  }),
  scalar_localizedtext: new Variant({
    dataType: DataType.LocalizedText,
    value: coerceLocalizedText("Hello World"),
  }),
  scalar_qualifiedname: new Variant({
    dataType: DataType.QualifiedName,
    value: coerceQualifiedName("Hello"),
  }),
};

async function initializeNodes(session: ClientSession, nodes: Record<string, string>) {
  console.log("Initializing nodes (", Object.keys(nodes).length, ")");

  const nodesToWrite: WriteValueOptions[] = Object.keys(nodes).map((key) => {
    const v = new Variant(defaultValue[key]);
    const n = nodes[key];
    return new WriteValue({
      nodeId: n,
      attributeId: AttributeIds.Value,
      value: {
        value: v,
      },
    });
  });

  const results = await session.write(nodesToWrite);

  const bad = _.zip(nodesToWrite, results).filter((pair) => pair[1] != StatusCodes.Good);
  if (bad.length > 0) {
    bad.forEach((x) => {
      console.log(x[0].nodeId?.toString(), x[0].value?.value?.toString() + " " + x[1].toString());
    });
    throw new Error("Invalid write on node of types : " + bad.map((x) => x[0].value?.value?.dataType?.toString()).join(" "));
  }
  const dataValues = await session.read(nodesToWrite);
  for (let i = 0; i < nodesToWrite.length && i < 2; i++) {
    console.log(nodesToWrite[i].value!.value!.toString(), dataValues[i].value.toString());
  }
}

async function main() {

  if (true) {
    const nodes = filterNodes(config.nodes_uaautomation_cpp);
    const endpointUrl = "opc.tcp://" + hostname + ":48010";
    await benchmark("UA Automation CPP", nodes, endpointUrl);
  }

  if (true) {
    const nodes = filterNodes(config.nodes_node_opcua);
    // let hostname = "opcuademo.sterfive.com";
    const endpointUrl = "opc.tcp://" + hostname + ":26544";
    await benchmark("NodeOPCUA(2.20.0)-CURRENT", nodes, endpointUrl);
  }
  if (true) {
    const nodes = filterNodes(config.nodes_node_opcua);
    // let hostname = "opcuademo.sterfive.com";
    const endpointUrl = "opc.tcp://" + hostname + ":26543";
    await benchmark("NodeOPCUA(2.21.0)-DEV", nodes, endpointUrl);
  }
  if (false) {
    const nodes = filterNodes(config.nodes_node_opcua);
    let hostname = "opcuademo.sterfive.com";
    const endpointUrl = "opc.tcp://" + hostname + ":26543";
    await benchmark("NodeOPCUA(0.0.65)", nodes, endpointUrl);
  }

  if (false) {
    const nodes = filterNodes(config.nodes_openopcua);
    const endpointUrl = "opc.tcp://" + hostname + ":16664";
    await benchmark("OpenOPCUA", nodes, endpointUrl);
  }

  if (false) {
    const nodes = filterNodes(config.nodes_uaautomation_ansiC);
    const endpointUrl = "opc.tcp://" + hostname + ":48020";
    await benchmark("UA Automation AnsiC", nodes, endpointUrl);
  }

  if (true) {
    const nodes = filterNodes(config.nodes_prosys);
    const endpointUrl = "opc.tcp://" + hostname + ":53530/OPCUA/SimulationServer";
    await benchmark("PROSYS", nodes, endpointUrl);
  }
  if (false) {
    const nodes = filterNodes(config.generic_opc);
    const endpointUrl = "opc.tcp://" + "localhost" + ":62541/Quickstarts/ReferenceServer";
    await benchmark("OPCFoundation", nodes, endpointUrl);
  }
  if (false) {
    const nodes = filterNodes(config.nodes_milo);
    const endpointUrl = "opc.tcp://" + hostname + ":12686/example";
    await benchmark("Milo", nodes, endpointUrl);
  }

  console.log("Results");
  const table: string[][] = [];
  // push column headers:
  table.push(["Stack"]);
  samples.forEach((c) => {
    table[0].push("Read " + c);
  });
  table[0].push("Bytes Written");
  table[0].push("Chunks Written");
  table[0].push("Bytes Read");
  table[0].push("Chunks Read");

  Object.keys(g_results).map((stack) => {
    const row = [stack];
    samples.forEach((c) => {
      const r = g_results[stack][c];
      row.push(r ? r.nbReadPerSecGlobal.toFixed(0) : "---");
    });

    const r = g_results[stack][samples[0]];
    row.push(r ? r.bytesWritten.toFixed(0) : "---");
    row.push(r ? r.chunkWrittenCount.toFixed(0) : "---");
    row.push(r ? r.bytesRead.toFixed(0) : "---");
    row.push(r ? r.chunkReadCount.toFixed(0) : "---");
    table.push(row);
  });

  console.log(markdownTable(table, { align: "r" }));

  console.log("----------------------->");
  process.exit(0);
}

main();
