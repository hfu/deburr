const config = require('config')
const Queue = require('better-queue')
const Database = require('better-sqlite3')
const fs = require('fs')
const winston = require('winston')
const path = require('path')
const pretty = require('prettysize')

// global settings
winston.add(new winston.transports.Console({
  format: winston.format.simple() 
}))

// global constants
const concurrent = config.get('concurrent')
const minzoom = config.get('minzoom')
const maxzoom = config.get('maxzoom')

// functions
const yflip = (y, z) => {
  return (1 << z) - 1 - y
}

const iso = () => {
  return new Date().toISOString()
}

const upperLeftTMS = (zxy, z2) => {
  return [
    z2,
    (1 << (z2 - zxy[0])) * zxy[1],
    yflip((1 << (z2 - zxy[0])) * zxy[2], z2)
  ]
}

const deburr = (db, zxy) => {
  for (let z = minzoom; z <= maxzoom; z++) {
    const p0 = upperLeftTMS(zxy, z)
    const nextZxy = [zxy[0], zxy[1] + 1, zxy[2] + 1]
    const p1 = upperLeftTMS(nextZxy, z)
    // winston.info(`${zxy} -> ${p0} / ${p1}`)
    const sql = `DELETE FROM tiles WHERE  
zoom_level = ${z} AND NOT (
tile_column BETWEEN ${p0[1]} AND ${p1[1] - 1} AND
tile_row BETWEEN ${p1[2] + 1} AND ${p0[2]});` 
    db.exec(sql)
    winston.info(`${z}`)
  }
}

const queue = new Queue((mbtilesPath, cb) => {
  if (!fs.existsSync(mbtilesPath)) {
    return cb(null)
  }
  winston.info(`${iso()}: processing ${path.basename(mbtilesPath)} ${pretty(fs.statSync(mbtilesPath).size)}`)
  const db = new Database(mbtilesPath, {})
  zxy = path.basename(mbtilesPath, '.mbtiles')
    .split('-').map(v => Number(v))
  deburr(db, zxy)
  db.exec('VACUUM;')
  db.close()
  winston.info(` -> ${pretty(fs.statSync(mbtilesPath).size)}`)
  return cb(null)
}, { concurrent: concurrent })

queue.on('failed', err => {
  winston.info(err)
})

// main
for (let i = 2; i <= process.argv.length; i++) {
  queue.push(process.argv[i])
}
