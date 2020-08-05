// Libraries
import {SFC} from 'react'

// Constants
import {CLIENT_LIBS, TELEGRAF_PLUGINS} from 'src/shared/constants/routes'

// Graphics
import {
  ArduinoLogo,
  CSharpLogo,
  GoLogo,
  JavaLogo,
  JSLogo,
  KotlinLogo,
  PHPLogo,
  PythonLogo,
  RubyLogo,
  ScalaLogo,
} from 'src/clientLibraries/graphics'

// Types
export interface WriteDataItem {
  id: string
  name: string
  url: string
  image?: SFC
}

export interface WriteDataSection {
  id: string
  name: string
  description: string
  items: WriteDataItem[]
}

// Section Items
// NOTE: url begins with `orgs/orgid/load-data/`
export const WRITE_DATA_CLIENT_LIBRARIES: WriteDataItem[] = [
  {
    id: 'arduino',
    name: 'Arduino',
    url: `${CLIENT_LIBS}/arduino`,
    image: ArduinoLogo,
  },
  {
    id: 'csharp',
    name: 'C#',
    url: `${CLIENT_LIBS}/csharp`,
    image: CSharpLogo,
  },
  {
    id: 'go',
    name: 'GO',
    url: `${CLIENT_LIBS}/go`,
    image: GoLogo,
  },
  {
    id: 'java',
    name: 'Java',
    url: `${CLIENT_LIBS}/java`,
    image: JavaLogo,
  },
  {
    id: 'javascript-node',
    name: 'JavaScript/Node.js',
    url: `${CLIENT_LIBS}/javascript-node`,
    image: JSLogo,
  },
  {
    id: 'python',
    name: 'Python',
    url: `${CLIENT_LIBS}/python`,
    image: PythonLogo,
  },
  {
    id: 'ruby',
    name: 'Ruby',
    url: `${CLIENT_LIBS}/ruby`,
    image: RubyLogo,
  },
  {
    id: 'php',
    name: 'PHP',
    url: `${CLIENT_LIBS}/php`,
    image: PHPLogo,
  },
  {
    id: 'kotlin',
    name: 'Kotlin',
    url: `${CLIENT_LIBS}/kotlin`,
    image: KotlinLogo,
  },
  {
    id: 'scala',
    name: 'Scala',
    url: `${CLIENT_LIBS}/scala`,
    image: ScalaLogo,
  },
]

export const WRITE_DATA_TELEGRAF_PLUGINS: WriteDataItem[] = [
  {
    id: 'bcache',
    name: 'Bcache',
    url: `${TELEGRAF_PLUGINS}/bcache`,
  },
]

export const WRITE_DATA_INTEGRATIONS: WriteDataItem[] = [
  {
    id: 'integrationa',
    name: 'IntegrationA',
    url: '',
  },
]

// Sections
export const WRITE_DATA_SECTIONS: WriteDataSection[] = [
  {
    id: 'client-libraries',
    name: 'Client Libraries',
    description: 'Back-end, front-end, and mobile applications',
    items: WRITE_DATA_CLIENT_LIBRARIES,
  },
  {
    id: 'telegraf-plugins',
    name: 'Telegraf Plugins',
    description: 'Description goes here',
    items: WRITE_DATA_TELEGRAF_PLUGINS,
  },
  {
    id: 'integrations',
    name: 'Integrations',
    description: 'Description goes here',
    items: WRITE_DATA_INTEGRATIONS,
  },
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
