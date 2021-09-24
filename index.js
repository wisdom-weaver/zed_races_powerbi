const axios = require("axios");
const _ = require("lodash");
const cron = require("node-cron");
const cron_parser = require("cron-parser");
const appRootPath = require("app-root-path");
const { parse } = require("json2csv");
const fs = require("fs");
const path = require("path");
require("dotenv").config();

const zed_gql = "https://zed-ql.zed.run/graphql/getRaceResults";
const zed_secret_key = process.env.zed_secret_key;
const mt = 60 * 1000;
const hr = 1000 * 60 * 60;
const day_diff = 1000 * 60 * 60 * 24 * 1;
const g_h = 72;

const cron_conf = {
  scheduled: true,
};
const def_config = {
  g_odds_zero: true,
};

const key_mapping_bs_zed = [
  ["1", "distance"],
  ["2", "date"],
  ["3", "entryfee"],
  ["4", "raceid"],
  ["5", "race_class"],
  ["6", "hid"],
  ["7", "finishtime"],
  ["8", "place"],
  ["9", "name"],
  ["10", "gate"],
  ["11", "horse_class"],
];

const struct_race_row_data = (data) => {
  try {
    // console.log(data.length);
    if (_.isEmpty(data)) return [];
    data = data?.map((row) => {
      // console.log(row);
      if (row == null) return null;
      return key_mapping_bs_zed.reduce(
        (acc, [key_init, key_final]) => ({
          ...acc,
          [key_final]: row[key_init] || 0,
        }),
        {}
      );
    });
    data = _.compact(data);
  } catch (err) {
    if (data.name == "MongoNetworkError") {
      console.log("MongoNetworkError");
    }
    return [];
  }
  return data;
};

const write_to_path = ({ file_path, data }) => {
  if (!fs.existsSync(path.dirname(file_path)))
    fs.mkdirSync(path.dirname(file_path), { recursive: true });
  fs.writeFileSync(file_path, JSON.stringify(data, null, 2));
};

const write_to_path_txt = ({ file_path, data }) => {
  if (!fs.existsSync(path.dirname(file_path)))
    fs.mkdirSync(path.dirname(file_path), { recursive: true });
  fs.writeFileSync(file_path, data);
};

const read_from_path = ({ file_path }) => {
  try {
    if (!fs.existsSync(file_path)) return null;
    json = fs.readFileSync(file_path, "utf8");
    if (_.isEmpty(json)) return null;
    return JSON.parse(json) || null;
  } catch (err) {
    return null;
  }
};

const get_zed_raw_data = async (from, to) => {
  try {
    let arr = [];
    let json = {};
    let headers = {
      "x-developer-secret": zed_secret_key,
      "Content-Type": "application/json",
    };

    let payload = {
      query: `query ($input: GetRaceResultsInput, $before: String, $after: String, $first: Int, $last: Int) {
  getRaceResults(before: $before, after: $after, first: $first, last: $last, input: $input) {
    edges {
      cursor
      node {
        name
        length
        startTime
        fee
        raceId
        status
        class

        horses {
          horseId
          finishTime
          finalPosition
          name
          gate
          ownerAddress
          class

        }
      }
    }
  }
}`,
      variables: {
        first: 500,
        input: {
          dates: {
            from: from,
            to: to,
          },
        },
      },
    };

    let axios_config = {
      method: "post",
      url: zed_gql,
      headers: headers,
      data: JSON.stringify(payload),
    };
    let result = await axios(axios_config);
    let edges = result?.data?.data?.getRaceResults?.edges || [];
    let racesData = {};
    for (let edgeIndex in edges) {
      let edge = edges[edgeIndex];
      let node = edge.node;
      let node_length = node.length;
      let node_startTime = node.startTime;
      let node_fee = node.fee;
      let node_raceId = node.raceId;
      let node_class = node.class;
      let horses = node.horses;

      racesData[node_raceId] = {};
      for (let horseIndex in horses) {
        let horse = horses[horseIndex];
        let horses_horseId = horse.horseId;
        let horses_finishTime = horse.finishTime;
        let horses_finalPosition = horse.finalPosition;
        let horses_name = horse.name;
        let horses_gate = horse.gate;
        let horses_class = horse.class;

        racesData[node_raceId][horses_horseId] = {
          1: node_length,
          2: node_startTime,
          3: node_fee,
          4: node_raceId,
          5: node_class,
          6: horses_horseId,
          7: horses_finishTime,
          8: horses_finalPosition,
          9: horses_name,
          10: horses_gate,
          11: horses_class,
        };
      }
    }

    return racesData;
  } catch (err) {
    console.log("get_zed_raw_data err", err);
    return [];
  }
};

const race_write_path_csv = ({ date, rid }) =>
  `${appRootPath}/data/races_csv/${date}-${rid}.csv`;
const race_write_path_json = ({ date, rid }) =>
  `${appRootPath}/data/races_json/${date}-${rid}.json`;

const write_race = async ({ rid, race }) => {
  try {
    race = _.values(race);
    race = struct_race_row_data(race);
    race = _.sortBy(race, "gate");
    let date = race[0].date;
    const opts = { fields: _.keys(race[0]) };
    const csv = parse(race, opts);
    const file_path_csv = race_write_path_csv({ rid, date });
    const file_path_json = race_write_path_json({ rid, date });
    const json = {
      rid,
      race,
    };
    write_to_path_txt({ file_path: file_path_csv, data: csv });
    write_to_path({ file_path: file_path_json, data: json });
  } catch (err) {
    console.log("err writing race", rid);
  }
};

const write_races_chunk = async (races) => {
  if (_.isEmpty(races)) return;
  let mongo_push = [];
  for (let r in races) {
    await write_race({ rid: r, race: races[r] });
  }
  // console.log(mongo_push);
  // await zed_ch.db.collection("zed").bulkWrite(mongo_push);
};

const zed_race_add_runner = async (mode = "auto", dates, config) => {
  // console.log(mode);
  let ob = {};
  let now = Date.now();
  let from, to;
  if (mode == "auto") {
    from = new Date(now - mt * 5).toISOString();
    to = new Date(now).toISOString();
  } else if (mode == "manual") {
    let { from_a, to_a } = dates || {};

    from = new Date(from_a).toISOString();
    to = new Date(to_a).toISOString();
  }
  let date_str = new Date(from).toISOString().slice(0, 10);
  let f_s = new Date(from).toISOString().slice(11, 19);
  let t_s = new Date(to).toISOString().slice(11, 19);
  try {
    console.log("\n#", date_str, f_s, "->", t_s);
    let races = await get_zed_raw_data(from, to);
    if (_.keys(races).length == 0) {
      console.log("no races");
      return;
    }

    let min_races = [];
    let rids = _.keys(races);
    console.log("fetched", rids.length, "race ids");

    for (let [rid, r] of _.entries(races)) {
      if (_.isEmpty(r)) {
        console.log("err", rid, "empty race");
        continue;
      }

      let l = _.values(r).length;
      if (l == 0) {
        console.log("ERROR EMPTY       ", _.values(r)[0][2], rid, `${l}_h`);
      } else if (l < 12) {
        console.log("WARN less than 12:", _.values(r)[0][2], rid, `${l}_h`);
        min_races.push([rid, r]);
      } else {
        console.log("getting race from:", _.values(r)[0][2], rid, `${l}_h`);
        min_races.push([rid, r]);
      }
    }
    races = _.chain(min_races).compact().fromPairs().value();
    // console.log(races)
    await write_races_chunk(races);
    console.log("done", _.keys(races).length, "races");
  } catch (err) {
    console.log("ERROR on zed_race_add_runner", err.message);
  }
};

const zed_races_automated_script_run = async () => {
  console.log("\n## zed_races_script_run started");
  let cron_str = "*/2 * * * *";
  const c_itvl = cron_parser.parseExpression(cron_str);
  console.log("Next run:", c_itvl.next().toISOString(), "\n");
  cron.schedule(
    cron_str,
    () => zed_race_add_runner("auto", def_config),
    cron_conf
  );
};
// zed_races_automated_script_run();

const test = async () => {
  let rid = "DZ9eKv9c";
  let date = "2021-08-24T01:53:41Z";
  let dates = {
    from_a: date,
    to_a: date,
  };
  await zed_race_add_runner("manual", dates);
  console.log("done");
};

const runner = async () => {
  console.log("started");
  // await test();
  // zed_races_automated_script_run();
  // zed_race_add_runner("auto", def_config)
  console.log("done");
};
// runner();

module.exports = {
  zed_secret_key,
  zed_race_add_runner,
  zed_races_automated_script_run
};
