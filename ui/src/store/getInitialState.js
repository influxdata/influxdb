import uuid from 'node-uuid'
import defaultQueryConfig from 'src/utils/defaultQueryConfig'

export default function getInitialState() {
  const panelID = uuid.v4() // for the default Panel
  const queryID = uuid.v4() // for the default Query
  return {
    panels: {
      [panelID]: {
        id: panelID,
        queryIds: [queryID],
      },
    },
    queryConfigs: {
      [queryID]: defaultQueryConfig(queryID),
    },
  }
}
