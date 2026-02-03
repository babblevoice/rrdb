
/*
 Tests taken from the initial python script, converted to mocha. More needed.
*/

import { execFile } from "node:child_process"
import { promisify } from "node:util"
import { randomUUID } from "node:crypto"
import fs from "node:fs/promises"
import { expect } from "chai"

const execFileAsync = promisify(execFile)

// Adjust if your rrdb lives elsewhere
const RRDB_BIN = "/usr/bin/rrdb"

// Small helper to run rrdb with args, returning stdout
async function rrdb(args, opts = {}) {
  const { stdout, stderr } = await execFileAsync(RRDB_BIN, args, opts)
  // Helpful when debugging locally
  if (stderr && stderr.trim()) {
    // console.error(stderr)
  }
  return stdout
}

// Parse "timestamp:value" -> numeric value
function parseValue(line) {
  // matches any leading timestamp then colon then value to end of line
  const m = line.match(/^.*?:(-?\d+(?:\.\d+)?)\s*$/i)
  if (!m) throw new Error(`Failed to parse line: ${line}`)
  return Number(m[1])
}

function genFilename() {
  return `${randomUUID()}.rrdb`
}

describe( "rrdb xform suite", function () {
  this.timeout( 120000 ) // creating + 1500 execs isn’t instant

  const env = {
      ...process.env,
      FAKETIME: "@2025-10-31 12:00:00",
      LD_PRELOAD: "/usr/lib/faketime/libfaketime.so.1"
    }

  const dir = "/tmp"
  const filename = genFilename()

  // expected accumulators
  const totalCount = 1500
  let totalsum0 = 0
  let totalsum1 = 0
  let max0 = 0
  let max1 = 0

  // convenience fetcher by xform index returns numeric value (second column)
  async function fetchXform(index) {
    const out = await rrdb(
      [
        "--command=fetch",
        `--dir=${dir}/`,
        `--filename=${filename}`,
        `--xform=${index}`,
      ],
      { env }
    )
    // fetch for xform returns exactly one line "ts:value"
    const line = out.trim().split("\n")[0] ?? ""
    return parseValue(line)
  }

  async function fetchinfo() {
    const out = await rrdb(
      [
        "--command=info",
        `--dir=${dir}/`,
        `--filename=${filename}`,
      ],
      { env }
    )

    return out.trim().split("\n") ?? []
  }

  before(async function () {
    // build xforms list exactly like the Python intended (fixed the missing colon bug)
    // 0 RRDBCOUNT:ONEDAY
    // 1 RRDBCOUNT:FIVEMINUTE
    // 2 RRDBSUM:FIVEMINUTE:0
    // 3 RRDBMAX:FIVEMINUTE:0
    // 4 RRDBMEAN:FIVEMINUTE:0
    // 5 RRDBMEAN:ONEDAY:0
    // 6 RRDBMEAN:FIVEMINUTE:1
    // 7 RRDBMEAN:ONEDAY:1
    // 8 RRDBMAX:QUARTERHOUR:0
    const xforms = [
      "RRDBCOUNT:ONEDAY",
      "RRDBCOUNT:FIVEMINUTE",
      "RRDBSUM:FIVEMINUTE:0",
      "RRDBMAX:FIVEMINUTE:0",
      "RRDBMEAN:FIVEMINUTE:0",
      "RRDBMEAN:ONEDAY:0",
      "RRDBMEAN:FIVEMINUTE:1",
      "RRDBMEAN:ONEDAY:1",
      "RRDBMAX:QUARTERHOUR:0",
    ].join(":")

    // Create DB with 2 sets and 500 samples like Python
    await rrdb(
      [
        "--command=create",
        `--dir=${dir}/`,
        `--filename=${filename}`,
        "--setcount=2",
        "--samplecount=500",
        `--xform=${xforms}`,
      ],
      { env }
    )

    // Slam in 1500 updates, track sums and maxima the same way
    // Note: randint in Python was inclusive mirror that here.
    function randIntInclusive(min, max) {
      return Math.floor(Math.random() * (max - min + 1)) + min
    }

    for (let n = 0; n < totalCount; n++) {
      const v0 = randIntInclusive(0, 20000)
      const v1 = randIntInclusive(0, 1_000_000)
      totalsum0 += v0
      totalsum1 += v1
      if (v0 > max0) max0 = v0
      if (v1 > max1) max1 = v1

      await rrdb(
        [
          "--command=update",
          `--dir=${dir}/`,
          `--filename=${filename}`,
          `--value=${v0}:${v1}`,
        ],
        { env }
      )
    }
  })

  after(async function () {
    // attempt cleanup if it fails, not the end of the world
    try {
      await fs.unlink(`${dir}/${filename}`)
    } catch {
      /* ignore */
    }
  })

  it("xform 0 RRDBCOUNT:ONEDAY equals total updates", async function () {
    const val = await fetchXform(0)
    expect(val).to.equal(totalCount)
  })

  it("xform 1 RRDBCOUNT:FIVEMINUTE equals total updates", async function () {
    const val = await fetchXform(1)
    expect(val).to.equal(totalCount)
  })

  it("xform 2 RRDBSUM:FIVEMINUTE:0 equals sum of set 0", async function () {
    const val = await fetchXform(2)
    expect(val).to.equal(totalsum0)
  })

  it("xform 3 RRDBMAX:FIVEMINUTE:0 equals max of set 0", async function () {
    const val = await fetchXform(3)
    expect(val).to.equal(max0)
  })

  it("xform 4 RRDBMEAN:FIVEMINUTE:0 equals mean of set 0", async function () {
    const val = await fetchXform(4)
    const mean = totalsum0 / totalCount
    expect(val).to.be.closeTo(mean, 0.001)
  })

  it("xform 5 RRDBMEAN:ONEDAY:0 equals mean of set 0", async function () {
    const val = await fetchXform(5)
    const mean = totalsum0 / totalCount
    expect(val).to.be.closeTo(mean, 0.001)
  })

  it("xform 6 RRDBMEAN:FIVEMINUTE:1 equals mean of set 1", async function () {
    const val = await fetchXform(6)
    const mean = totalsum1 / totalCount
    expect(val).to.be.closeTo(mean, 0.001)
  })

  it("xform 7 RRDBMEAN:ONEDAY:1 equals mean of set 1", async function () {
    const val = await fetchXform(7)
    const mean = totalsum1 / totalCount
    expect(val).to.be.closeTo(mean, 0.001)
  })

  it("xform 8 RRDBMAX:QUARTERHOUR:0 equals max of set 0", async function () {
    const val = await fetchXform(8)
    expect(val).to.equal(max0)
  })

  it("xform rrdb file info", async function () {
    const val = await fetchinfo()

    const expected = [
      'Version is 1',
      'Number of sets 2',
      'Number of samples 500',
      'Current window position 0',
      'Contains #9 xformations',
      'RRDBCOUNT:ONEDAY',
      'RRDBCOUNT:FIVEMINUTE',
      'RRDBSUM:FIVEMINUTE',
      'RRDBMAX:FIVEMINUTE',
      'RRDBMEAN:FIVEMINUTE',
      'RRDBMEAN:ONEDAY',
      'RRDBMEAN:FIVEMINUTE',
      'RRDBMEAN:ONEDAY',
      'RRDBMAX:QUARTERHOUR'
    ]
    expect(val).to.eql(expected)
  })
})
