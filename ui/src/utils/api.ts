import Client from '@influxdata/influx'

const basePath = '/api/v2'

export const client = new Client(basePath)
