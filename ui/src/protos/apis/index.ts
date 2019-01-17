// API funcs
import {protosAPI} from 'src/utils/api'

// types
import {Protos, Dashboards} from 'src/api'

export const getProtos = async (): Promise<Protos> => {
  const {data} = await protosAPI.protosGet()

  return data
}

export const createDashFromProto = async (
  protoID: string,
  orgID: string
): Promise<Dashboards> => {
  const {data} = await protosAPI.protosProtoIDDashboardsPost(protoID, {orgID})
  // might want to map dashboard[] to more one more useful to FE.
  return data
}
