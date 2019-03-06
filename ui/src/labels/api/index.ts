// API
import {client} from 'src/utils/api'

// Types
import {ILabel} from '@influxdata/influx'

export const createLabelAJAX = async (newLabel: ILabel): Promise<ILabel> => {
  return await client.labels.create(newLabel.name, newLabel.properties)
}
