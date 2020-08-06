// Constants
import {CLIENT_LIBS} from 'src/shared/constants/routes'

// Types
import {WriteDataItem, WriteDataSection} from 'src/writeData/constants'

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

const WRITE_DATA_CLIENT_LIBRARIES_SECTION: WriteDataSection = {
  id: CLIENT_LIBS,
  name: 'Client Libraries',
  description: 'Back-end, front-end, and mobile applications',
  items: WRITE_DATA_CLIENT_LIBRARIES,
}

export default WRITE_DATA_CLIENT_LIBRARIES_SECTION
