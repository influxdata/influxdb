import {queryAPI} from 'src/utils/api'
import {Package} from 'src/types/ast'

export const getAST = async (query: string): Promise<Package> => {
  const {data} = await queryAPI.queryAstPost(undefined, undefined, {query})

  return data.ast
}
