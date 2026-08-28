const hb = require('handlebars')
const client = require('../clickhouse')
const logger = require('../../logger')
const {samplesOrderingRule} = require('../../../common')
const getEnv = () => {
  return {
    CLICKHOUSE_DB: 'cloki',
    LABELS_DAYS: 7,
    SAMPLES_DAYS: 7,
    ...process.env
  }
}

/**
 *
 * @param db {string}
 * @returns {Promise<void>}
 */
module.exports.upgrade = async (db) => {
  const scripts = require('./scripts')
  await upgradeSingle(db, 1, scripts.overall)
  await upgradeSingle(db, 2, scripts.traces)
}
/**
 *
 * @param db {string}
 * @param key {number}
 * @param scripts {string[]}
 */
const upgradeSingle = async (db, key, scripts) => {
  await client.rawRequest(`CREATE DATABASE IF NOT EXISTS ${db}`)
  await client.rawRequest('CREATE TABLE IF NOT EXISTS ver (k UInt64, ver UInt64) ' +
    'ENGINE=ReplicatedReplacingMergeTree(ver) ORDER BY k', null, db)
  let ver = await client.rawRequest(`SELECT max(ver) as ver FROM ver WHERE k = ${key} FORMAT JSON`,
    null, db)
  ver = ver.data.data && ver.data.data[0] && ver.data.data[0].ver ? ver.data.data[0].ver : 0
  for (let i = parseInt(ver); i < scripts.length; ++i) {
    if (!scripts[i]) { continue }
    scripts[i] = scripts[i].trim()
    const tpl = hb.compile(scripts[i])
    scripts[i] = tpl({ ...getEnv(), DB: db, SAMPLES_ORDER_RUL: samplesOrderingRule() })
    console.log(`v${i} -> v${i + 1}`)
    console.log(scripts[i])
    await client.rawRequest(scripts[i], null, db)
    await client.rawRequest(`INSERT INTO ver (k, ver) VALUES (${key}, ${i + 1})`, null, db)
  }
}

/**
 * @param opts {{db: string, samples_days: number, time_series_days: number, storage_policy: string}[]}
 * @returns {Promise<void>}
 */
module.exports.rotate = async (opts) => {
  for (const db of opts) {
    const settings = await client.getSettings([
      { type: 'rotate', name: 'v3_samples_days' },
      { type: 'rotate', name: 'v3_time_series_days' },
      { type: 'rotate', name: 'v3_storage_policy' },
      { type: 'rotate', name: 'v1_traces_days' },
      { type: 'rotate', name: 'v1_traces_storage_policy' }
    ], db.db)
    if (db.samples_days + '' !== settings.v3_samples_days) {
      const alterTable = 'ALTER TABLE samples_v3 MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateTable = `ALTER TABLE samples_v3 MODIFY TTL toDateTime(timestamp_ns / 1000000000) + INTERVAL ${db.samples_days} DAY`
      await client.rawRequest(alterTable, null, db.db)
      await client.rawRequest(rotateTable, null, db.db)
      await client.addSetting('rotate', 'v3_samples_days', db.samples_days + '', db.db)
    }
    if (db.time_series_days + '' !== settings.v3_time_series_days) {
      const alterTable = 'ALTER TABLE time_series MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateTable = `ALTER TABLE time_series MODIFY TTL "date" + INTERVAL ${db.time_series_days} DAY`
      await client.rawRequest(alterTable, null, db.db)
      await client.rawRequest(rotateTable, null, db.db)
      const alterView = 'ALTER TABLE time_series_gin MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateView = `ALTER TABLE time_series_gin MODIFY TTL "date" + INTERVAL ${db.time_series_days} DAY`
      await client.rawRequest(alterView, null, db.db)
      await client.rawRequest(rotateView, null, db.db)
      await client.addSetting('rotate', 'v3_time_series_days', db.time_series_days + '', db.db)
    }
    if (db.storage_policy && db.storage_policy !== settings.v3_storage_policy) {
      logger.debug(`Altering storage policy: ${db.storage_policy}`)
      const alterTs = `ALTER TABLE time_series MODIFY SETTING storage_policy='${db.storage_policy}'`
      const alterTsVw = `ALTER TABLE time_series_gin MODIFY SETTING storage_policy='${db.storage_policy}'`
      const alterSm = `ALTER TABLE samples_v3 MODIFY SETTING storage_policy='${db.storage_policy}'`
      await client.rawRequest(alterTs, null, db.db)
      await client.rawRequest(alterTsVw, null, db.db)
      await client.rawRequest(alterSm, null, db.db)
      await client.addSetting('rotate', 'v3_storage_policy', db.storage_policy, db.db)
    }
    if (db.samples_days + '' !== settings.v1_traces_days) {
      let alterTable = 'ALTER TABLE tempo_traces MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      let rotateTable = `ALTER TABLE tempo_traces MODIFY TTL toDateTime(timestamp_ns / 1000000000) + INTERVAL ${db.samples_days} DAY`
      await client.rawRequest(alterTable, null, db.db)
      await client.rawRequest(rotateTable, null, db.db)
      alterTable = 'ALTER TABLE tempo_traces_attrs_gin MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      rotateTable = `ALTER TABLE tempo_traces_attrs_gin MODIFY TTL date + INTERVAL ${db.samples_days} DAY`
      await client.rawRequest(alterTable, null, db.db)
      await client.rawRequest(rotateTable, null, db.db)
      alterTable = 'ALTER TABLE tempo_traces_kv MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      rotateTable = `ALTER TABLE tempo_traces_kv MODIFY TTL date + INTERVAL ${db.samples_days} DAY`
      await client.rawRequest(alterTable, null, db.db)
      await client.rawRequest(rotateTable, null, db.db)
      await client.addSetting('rotate', 'v1_traces_days', db.samples_days + '', db.db)
    }
    if (db.storage_policy && db.storage_policy !== settings.v1_traces_storage_policy) {
      logger.debug(`Altering storage policy: ${db.storage_policy}`)
      const alterTs = `ALTER TABLE tempo_traces MODIFY SETTING storage_policy='${db.storage_policy}'`
      const alterTsVw = `ALTER TABLE tempo_traces_attrs_gin MODIFY SETTING storage_policy='${db.storage_policy}'`
      const alterSm = `ALTER TABLE tempo_traces_kv MODIFY SETTING storage_policy='${db.storage_policy}'`
      await client.rawRequest(alterTs, null, db.db)
      await client.rawRequest(alterTsVw, null, db.db)
      await client.rawRequest(alterSm, null, db.db)
      await client.addSetting('rotate', 'v1_traces_storage_policy', db.storage_policy, db.db)
    }
  }
}
