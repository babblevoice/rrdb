import { execFile } from "node:child_process"
import { expect } from "chai"
import { promisify } from "node:util"
import { randomUUID } from "node:crypto"
const execFileAsync = promisify(execFile)

const rrbdbin = "/usr/bin/rrdb"

/**
 *
 * @returns { string }
 */
function genfilename() {
  return `${randomUUID()}.rrdb`
}

describe("rrdb touch", function () {
  it( "rrdb very simple touch then fetch at a fixed time", async function () {

    const env = {
      ...process.env,
      FAKETIME: "@2025-10-31 12:00:00",
      LD_PRELOAD: "/usr/lib/faketime/libfaketime.so.1"
    }

    const fn = genfilename()

    const touchflags = [
      "--command=touch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--touchpath=test",
      "--samplecount=10",
      "--setcount=1",
      "--period=FIVEMINUTE"
    ]


    const fetchflags = [
      "--command=fetch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--period=FIVEMINUTE",
      "--touchpath=test"
    ]

    await execFileAsync( rrbdbin, touchflags, { env } )

    const { stdout } = await execFileAsync( rrbdbin, fetchflags, { env } )

    expect( stdout ).to.match(/^1761912000:1\n$/i)
  } )

  it( "rrdb more complex additions", async function () {

    const env = {
      ...process.env,
      FAKETIME: "@2025-10-31 12:00:00",
      LD_PRELOAD: "/usr/lib/faketime/libfaketime.so.1"
    }

    const fn = genfilename()

    const touchflags = [
      "--command=touch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--touchpath=test",
      "--samplecount=10",
      "--setcount=1",
      "--period=FIVEMINUTE"
    ]


    const fetchflags = [
      "--command=fetch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--period=FIVEMINUTE",
      "--touchpath=test"
    ]

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:06:00"

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    const { stdout } = await execFileAsync( rrbdbin, fetchflags, { env } )

    expect( stdout ).to.match(/^1761912300:2\n1761912000:3\n$/i)
  } )

  it( "rrdb more fill samples", async function () {

    const env = {
      ...process.env,
      FAKETIME: "@2025-10-31 12:00:00",
      LD_PRELOAD: "/usr/lib/faketime/libfaketime.so.1"
    }

    const fn = genfilename()

    const touchflags = [
      "--command=touch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--touchpath=test",
      "--samplecount=10",
      "--setcount=1",
      "--period=FIVEMINUTE"
    ]


    const fetchflags = [
      "--command=fetch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--period=FIVEMINUTE",
      "--touchpath=test"
    ]

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:06:00"

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:11:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:16:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:21:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:26:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:31:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:36:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:41:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:46:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    const { stdout } = await execFileAsync( rrbdbin, fetchflags, { env } )


    const expected = [
      "1761914700:1",
      "1761914400:1",
      "1761914100:1",
      "1761913800:1",
      "1761913500:1",
      "1761913200:1",
      "1761912900:1",
      "1761912600:1",
      "1761912300:2",
      "1761912000:3"
    ].join("\n")

    expect( stdout.trim() ).to.equal( expected )
  } )


  it( "rrdb now wrap by 1", async function () {

    const env = {
      ...process.env,
      FAKETIME: "@2025-10-31 12:00:00",
      LD_PRELOAD: "/usr/lib/faketime/libfaketime.so.1"
    }

    const fn = genfilename()

    const touchflags = [
      "--command=touch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--touchpath=test",
      "--samplecount=10",
      "--setcount=1",
      "--period=FIVEMINUTE"
    ]


    const fetchflags = [
      "--command=fetch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--period=FIVEMINUTE",
      "--touchpath=test"
    ]

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:06:00"

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:11:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:16:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:21:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:26:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:31:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:36:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:41:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:46:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:51:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    const { stdout } = await execFileAsync( rrbdbin, fetchflags, { env } )

    const expected = [
      "1761915000:1",
      "1761914700:1",
      "1761914400:1",
      "1761914100:1",
      "1761913800:1",
      "1761913500:1",
      "1761913200:1",
      "1761912900:1",
      "1761912600:1",
      "1761912300:2"
    ].join( "\n" )

    expect( stdout.trim() ).to.equal( expected )
  } )


  it( "rrdb now wrap by 2", async function () {

    const env = {
      ...process.env,
      FAKETIME: "@2025-10-31 12:00:00",
      LD_PRELOAD: "/usr/lib/faketime/libfaketime.so.1"
    }

    const fn = genfilename()

    const touchflags = [
      "--command=touch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--touchpath=test",
      "--samplecount=10",
      "--setcount=1",
      "--period=FIVEMINUTE"
    ]


    const fetchflags = [
      "--command=fetch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--period=FIVEMINUTE",
      "--touchpath=test"
    ]

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:06:00"

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:11:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:16:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:21:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:26:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:31:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:36:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:41:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:46:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:56:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    const { stdout } = await execFileAsync( rrbdbin, fetchflags, { env } )

    const expected = [
      "1761915300:1",
      "1761914700:1",
      "1761914400:1",
      "1761914100:1",
      "1761913800:1",
      "1761913500:1",
      "1761913200:1",
      "1761912900:1",
      "1761912600:1"
    ].join( "\n" )

    expect( stdout.trim() ).to.equal( expected )
  } )

  it( "rrdb now wrap by 4", async function () {

    const env = {
      ...process.env,
      FAKETIME: "@2025-10-31 12:00:00",
      LD_PRELOAD: "/usr/lib/faketime/libfaketime.so.1"
    }

    const fn = genfilename()

    const touchflags = [
      "--command=touch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--touchpath=test",
      "--samplecount=10",
      "--setcount=1",
      "--period=FIVEMINUTE"
    ]


    const fetchflags = [
      "--command=fetch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--period=FIVEMINUTE",
      "--touchpath=test"
    ]

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:06:00"

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:11:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:16:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:21:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:26:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:31:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:36:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:41:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:46:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 13:06:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    const { stdout } = await execFileAsync( rrbdbin, fetchflags, { env } )

    const expected = [
      "1761915900:1",
      "1761914700:1",
      "1761914400:1",
      "1761914100:1",
      "1761913800:1",
      "1761913500:1",
      "1761913200:1"
    ].join( "\n" )

    expect( stdout.trim() ).to.equal( expected )
  } )

  it( "rrdb now wrap by twice sample size", async function () {

    const env = {
      ...process.env,
      FAKETIME: "@2025-10-31 12:00:00",
      LD_PRELOAD: "/usr/lib/faketime/libfaketime.so.1"
    }

    const fn = genfilename()

    const touchflags = [
      "--command=touch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--touchpath=test",
      "--samplecount=10",
      "--setcount=1",
      "--period=FIVEMINUTE"
    ]


    const fetchflags = [
      "--command=fetch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--period=FIVEMINUTE",
      "--touchpath=test"
    ]

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:06:00"

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:11:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:16:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:21:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:26:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:31:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:36:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:41:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:46:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 13:46:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    const { stdout } = await execFileAsync( rrbdbin, fetchflags, { env } )

    const expected = [
      "1761918300:1"
    ].join( "\n" )

    expect( stdout.trim() ).to.equal( expected )
  } )

  it( "rrdb read quite a bit later and wrap by 1 during touch", async function () {

    /* only updates should cause a data removal */
    const env = {
      ...process.env,
      FAKETIME: "@2025-10-31 12:00:00",
      LD_PRELOAD: "/usr/lib/faketime/libfaketime.so.1"
    }

    const fn = genfilename()

    const touchflags = [
      "--command=touch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--touchpath=test",
      "--samplecount=10",
      "--setcount=1",
      "--period=FIVEMINUTE"
    ]


    const fetchflags = [
      "--command=fetch",
      "--dir=/tmp/",
      "--filename=" + fn,
      "--period=FIVEMINUTE",
      "--touchpath=test"
    ]

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:06:00"

    await execFileAsync( rrbdbin, touchflags, { env } )
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:11:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:16:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:21:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:26:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:31:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:36:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:41:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:46:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-10-31 12:51:00"
    await execFileAsync( rrbdbin, touchflags, { env } )

    env[ "FAKETIME" ] = "@2025-11-20 12:00:00"
    const { stdout } = await execFileAsync( rrbdbin, fetchflags, { env } )

    const expected = [
      "1761915000:1",
      "1761914700:1",
      "1761914400:1",
      "1761914100:1",
      "1761913800:1",
      "1761913500:1",
      "1761913200:1",
      "1761912900:1",
      "1761912600:1",
      "1761912300:2"
    ].join( "\n" )

    expect( stdout.trim() ).to.equal( expected )
  } )
} )