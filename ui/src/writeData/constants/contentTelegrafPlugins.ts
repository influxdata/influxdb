// Constants
import {TELEGRAF_PLUGINS} from 'src/shared/constants/routes'

// Types
import {WriteDataItem, WriteDataSection} from 'src/writeData/constants'

export const WRITE_DATA_TELEGRAF_PLUGINS: WriteDataItem[] = [
  {
    id: 'bcache',
    name: 'Bcache',
    url: `${TELEGRAF_PLUGINS}/bcache`,
  },
]

const WRITE_DATA_TELEGRAF_PLUGINS_SECTION: WriteDataSection = {
  id: TELEGRAF_PLUGINS,
  name: 'Telegraf Plugins',
  description: 'Description goes here',
  items: WRITE_DATA_TELEGRAF_PLUGINS,
}

export default WRITE_DATA_TELEGRAF_PLUGINS_SECTION
