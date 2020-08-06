// Constants
import {DEVELOPER_TOOLS} from 'src/shared/constants/routes'

// Types
import {WriteDataItem, WriteDataSection} from 'src/writeData/constants'

export const WRITE_DATA_DEVELOPER_TOOLS: WriteDataItem[] = [
  {
    id: 'dev-tool-a',
    name: 'Dev Tool A',
    url: `${DEVELOPER_TOOLS}/dev-tool-a`,
  },
]

const WRITE_DATA_DEVELOPER_TOOLS_SECTION: WriteDataSection = {
  id: DEVELOPER_TOOLS,
  name: 'Developer Tools',
  description: 'Description goes here',
  items: WRITE_DATA_DEVELOPER_TOOLS,
}

export default WRITE_DATA_DEVELOPER_TOOLS_SECTION
