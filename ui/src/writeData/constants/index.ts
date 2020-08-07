// Constants
import WRITE_DATA_CLIENT_LIBRARIES_SECTION from 'src/writeData/constants/contentClientLibraries'
import WRITE_DATA_TELEGRAF_PLUGINS_SECTION from 'src/writeData/constants/contentTelegrafPlugins'
import WRITE_DATA_INTEGRATIONS_SECTION from 'src/writeData/constants/contentIntegrations'
import WRITE_DATA_FLUX_SOURCES_SECTION from 'src/writeData/constants/contentFluxSources'
import WRITE_DATA_DEVELOPER_TOOLS_SECTION from 'src/writeData/constants/contentDeveloperTools'

// Types
export interface WriteDataItem {
  id: string
  name: string
  url: string
  image?: string
}

export interface WriteDataSection {
  id: string
  name: string
  description: string
  items: WriteDataItem[]
}

// Sections
export const WRITE_DATA_SECTIONS: WriteDataSection[] = [
  WRITE_DATA_CLIENT_LIBRARIES_SECTION,
  WRITE_DATA_TELEGRAF_PLUGINS_SECTION,
  WRITE_DATA_INTEGRATIONS_SECTION,
  WRITE_DATA_DEVELOPER_TOOLS_SECTION,
  WRITE_DATA_FLUX_SOURCES_SECTION,
]

// Functions
export const doesItemMatchSearchTerm = (
  itemName: string,
  searchTerm: string
): boolean => {
  return (
    itemName.toLowerCase().includes(searchTerm.toLowerCase()) ||
    searchTerm.toLowerCase().includes(itemName.toLowerCase())
  )
}

export const sectionContainsMatchingItems = (
  section: WriteDataSection,
  searchTerm: string
): boolean => {
  const matches = section.items.filter(item =>
    doesItemMatchSearchTerm(item.name, searchTerm)
  )

  return !!matches.length
}
