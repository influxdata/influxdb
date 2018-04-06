import {getRootNode} from 'src/utils/nodes'

export const getBasepath = () => {
  const rootNode = getRootNode()
  return rootNode.getAttribute('data-basepath') || ''
}
