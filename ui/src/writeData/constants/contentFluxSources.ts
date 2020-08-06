// Constants
import {FLUX_SOURCES} from 'src/shared/constants/routes'

// Types
import {WriteDataItem, WriteDataSection} from 'src/writeData/constants'

export const WRITE_DATA_FLUX_SOURCES: WriteDataItem[] = [
  {
    id: 'big-table',
    name: 'Big Table',
    url: `${FLUX_SOURCES}/big-table`,
  },
]

const WRITE_DATA_FLUX_SOURCES_SECTION: WriteDataSection = {
  id: FLUX_SOURCES,
  name: 'Flux Sources',
  description: 'Description goes here',
  items: WRITE_DATA_FLUX_SOURCES,
}

export default WRITE_DATA_FLUX_SOURCES_SECTION
