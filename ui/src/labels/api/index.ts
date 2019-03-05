// API
import {client} from 'src/utils/api'

// Types
import {Label} from '@influxdata/influx'

export const createLabelAJAX = async (newLabel: Label): Promise<Label> => {
  return await client.labels.create(newLabel.name, newLabel.properties)
}
