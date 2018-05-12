import AJAX from 'src/utils/ajax'
import {Source} from 'src/types'

export const writeLineProtocol = async (
  source: Source,
  db: string,
  data: string
): Promise<void> =>
  await AJAX({
    url: `${source.links.write}?db=${db}`,
    method: 'POST',
    data,
  })
