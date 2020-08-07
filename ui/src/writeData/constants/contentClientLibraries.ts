// Constants
import {CLIENT_LIBS} from 'src/shared/constants/routes'

// Types
import {WriteDataItem, WriteDataSection} from 'src/writeData/constants'

// Graphics
import arduinoLogo from 'src/writeData/graphics/arduinoLogo.svg'
import csharpLogo from 'src/writeData/graphics/csharpLogo.svg'
import goLogo from 'src/writeData/graphics/goLogo.svg'
import javaLogo from 'src/writeData/graphics/javaLogo.svg'
import nodeLogo from 'src/writeData/graphics/nodeLogo.svg'
import pythonLogo from 'src/writeData/graphics/pythonLogo.svg'
import rubyLogo from 'src/writeData/graphics/rubyLogo.svg'
import phpLogo from 'src/writeData/graphics/phpLogo.svg'
import kotlinLogo from 'src/writeData/graphics/kotlinLogo.svg'
import scalaLogo from 'src/writeData/graphics/scalaLogo.svg'

export const WRITE_DATA_CLIENT_LIBRARIES: WriteDataItem[] = [
  {
    id: 'arduino',
    name: 'Arduino',
    url: `${CLIENT_LIBS}/arduino`,
    image: arduinoLogo,
  },
  {
    id: 'csharp',
    name: 'C#',
    url: `${CLIENT_LIBS}/csharp`,
    image: csharpLogo,
  },
  {
    id: 'go',
    name: 'GO',
    url: `${CLIENT_LIBS}/go`,
    image: goLogo,
  },
  {
    id: 'java',
    name: 'Java',
    url: `${CLIENT_LIBS}/java`,
    image: javaLogo,
  },
  {
    id: 'javascript-node',
    name: 'JavaScript/Node.js',
    url: `${CLIENT_LIBS}/javascript-node`,
    image: nodeLogo,
  },
  {
    id: 'python',
    name: 'Python',
    url: `${CLIENT_LIBS}/python`,
    image: pythonLogo,
  },
  {
    id: 'ruby',
    name: 'Ruby',
    url: `${CLIENT_LIBS}/ruby`,
    image: rubyLogo,
  },
  {
    id: 'php',
    name: 'PHP',
    url: `${CLIENT_LIBS}/php`,
    image: phpLogo,
  },
  {
    id: 'kotlin',
    name: 'Kotlin',
    url: `${CLIENT_LIBS}/kotlin`,
    image: kotlinLogo,
  },
  {
    id: 'scala',
    name: 'Scala',
    url: `${CLIENT_LIBS}/scala`,
    image: scalaLogo,
  },
]

const WRITE_DATA_CLIENT_LIBRARIES_SECTION: WriteDataSection = {
  id: CLIENT_LIBS,
  name: 'Client Libraries',
  description: 'Back-end, front-end, and mobile applications',
  items: WRITE_DATA_CLIENT_LIBRARIES,
}

export default WRITE_DATA_CLIENT_LIBRARIES_SECTION
